/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.arrow.vector.types.pojo.ArrowType;
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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.rest.RestAttachAction;
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
    // table path and storage_options they point at. Tracked here so poll()
    // can extend append coverage to attach-only indexes and not just
    // namespace-registered tables (see C6: the previous behaviour was that
    // appending to a table whose index came from attach never surfaced
    // through _search until the operator hit refresh manually).
    private final Map<String, AttachedIndex> attachedIndexes = new ConcurrentHashMap<>();
    // Index names we've already flagged as unowned, so the poll doesn't shout
    // the same warning every ten seconds. Cleared if the collision resolves.
    private final Set<String> warnedUnowned = ConcurrentHashMap.newKeySet();
    // Track rename warnings so a table that renamed the same field is not
    // logged on every poll. Keyed by "indexName:fieldId:oldName->newName" so
    // the same rename fires once, but a later re-rename still warns.
    private final Set<String> warnedRenamed = ConcurrentHashMap.newKeySet();
    // Track which indexes we've already told the operator that the
    // `wait` uncovered-fragment policy is a no-op today. Set once per
    // index for the lifetime of the plugin instance; a poll every few
    // seconds would otherwise flood the log.
    private final Set<String> warnedWaitPolicy = ConcurrentHashMap.newKeySet();

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
        register(rootUri, StorageOptions.empty());
    }

    public void register(String rootUri, StorageOptions storageOptions) {
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
            namespaces.add(new RegisteredNamespace(rootUri, namespace, storageOptions));
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

    /**
     * Stops polling a previously-registered namespace. Surfaced indexes are
     * left in place — the operator can delete them separately if they want
     * the tables to disappear. Returns true if a registration matched, false
     * if the URI was not registered.
     */
    public boolean unregister(String rootUri) {
        return namespaces.removeIf(ns -> ns.rootUri.equals(rootUri));
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
                    syncTable(ns.rootUri, tableName, ns.storageOptions);
                }
            } catch (Exception e) {
                LOG.warn("namespace poll failed for {}", ns.rootUri, e);
            }
        }
        // Sync attach-created indexes so append fragments surface on the
        // same schedule as namespace-registered tables. attach records the
        // (indexName -> tablePath) pair; the sync path is the same, just
        // without the rootUri / tableName join namespace tables use.
        for (Map.Entry<String, AttachedIndex> entry : attachedIndexes.entrySet()) {
            AttachedIndex attached = entry.getValue();
            try {
                syncAttachedTable(entry.getKey(), attached.tablePath, attached.storageOptions);
            } catch (Exception e) {
                LOG.warn("attach poll failed for index {} at {}", entry.getKey(), attached.tablePath, e);
            }
        }
    }

    private void syncTable(String rootUri, String tableName, StorageOptions storageOptions) {
        String table = rootUri + "/" + tableName + ".lance";
        runSyncCycle(table, tableName, storageOptions);
    }

    // Attach-created indexes carry the fully-qualified table path already,
    // so the rootUri / tableName join namespace tables use doesn't apply.
    // Everything downstream of the path resolution is identical.
    private void syncAttachedTable(String indexName, String tablePath, StorageOptions storageOptions) {
        runSyncCycle(tablePath, indexName, storageOptions);
    }

    private void runSyncCycle(String table, String indexName, StorageOptions storageOptions) {
        try {
            boolean exists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
            if (!exists) {
                surface(indexName, table, storageOptions);
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
            try (Dataset dataset = LanceRegistry.openDataset(table, storageOptions)) {
                latest = dataset.version();
                if (latest > served) {
                    // The RFC's Mapping interface states the mapping is re-derived at
                    // every checkout. We derive first so the builder only touches
                    // columns that derived to lance_text; keyword columns stay untouched.
                    RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, null);
                    rederivedMappingJson = derivation.mappingJson();
                    warnOnLanceFieldRename(indexName, dataset.getLanceSchema());
                    if ("wait".equals(policy)) {
                        // The wait policy used to run Lance's incremental
                        // optimize here so appended fragments were folded
                        // into every existing index before the new version
                        // became visible. That behaviour committed extra
                        // versions to the user's Lance table (violating
                        // the "plugin never writes to a user table without
                        // permission" principle established by cf74c21) and
                        // pushed append visibility above nineteen minutes
                        // on 100M-row / 200-shard tables. The plugin now
                        // treats indexes as an external concern: they are
                        // expected to be built by the same writer that
                        // produced the table (Python, Ray, Spark, or the
                        // Lance Java SDK), and the plugin only helps out
                        // through the explicit
                        // `POST /_lance/build_indexes/{index}` endpoint.
                        // The wait value is still accepted so the setting
                        // shape can host a future async-optimize
                        // implementation; today it converges with the
                        // immediate branch. Log the observation once per
                        // index so an operator who set `wait` on purpose
                        // sees why nothing is happening.
                        warnDeprecatedWaitPolicyOnce(indexName);
                    }
                    // Either branch now exposes the new version at once
                    // and lets Lance fall back to scan evaluation on any
                    // fragment that the existing indexes have not yet
                    // caught up to. Lance's own scanner produces a mixed
                    // execution plan for FTS and knn (index for covered
                    // fragments, flat scan for uncovered fragments,
                    // unioned) so an incremental append never slows down
                    // the queries hitting the previously-covered
                    // fragments.
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
                                surface(indexName, table, storageOptions);
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

    private void surface(String indexName, String table, StorageOptions storageOptions) throws Exception {
        RestAttachAction.Derivation derivation;
        try (Dataset dataset = LanceRegistry.openDataset(table, storageOptions)) {
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
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", shards)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField());
        storageOptions.writeToSettings(settings);
        client.admin()
            .indices()
            .create(
                new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson()),
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
        registerAttachedIndex(indexName, tablePath, version, StorageOptions.empty());
    }

    public void registerAttachedIndex(String indexName, String tablePath, long version, StorageOptions storageOptions) {
        attachedIndexes.put(indexName, new AttachedIndex(tablePath, storageOptions));
        servedVersions.put(indexName, version);
    }

    /**
     * Compare the Lance table's current schema against the OpenSearch mapping
     * to detect column renames, schema resets (a field id reused with a
     * different Arrow type), and drops. Reactions:
     * <ul>
     *   <li>Rename (same id, same Arrow type, different name): log a warning
     *       once per session. The old name lingers because PutMapping cannot
     *       remove properties.</li>
     *   <li>Schema reset (same id, different Arrow type): log a drop + add
     *       pair, mark the stale name as {@code lance_dropped}, and let the
     *       mapping re-derivation add the new column.</li>
     *   <li>Drop (id gone from Lance): log once and mark the stale name as
     *       {@code lance_dropped}.</li>
     * </ul>
     * {@code lance_dropped} is stored in the field's {@code meta} so
     * {@link LanceTextFieldMapper}, {@link LanceVectorFieldMapper}, and any
     * future custom type can reject queries against it up front. Standard
     * scalar mappers (integer / keyword / date / boolean) do not honour it
     * yet — they still accept queries silently.
     */
    private void warnOnLanceFieldRename(String indexName, LanceSchema lanceSchema) {
        Map<Integer, MappingFieldInfo> mappingFieldIds;
        try {
            mappingFieldIds = readMappingFieldIds(indexName);
        } catch (Exception e) {
            LOG.debug("could not inspect mapping meta for {}: {}", indexName, e.getMessage());
            return;
        }
        if (mappingFieldIds.isEmpty()) {
            return;
        }
        // Collect field names to mark as dropped after the loop so we can
        // issue a single PutMapping call. Empty when no drift is observed.
        Set<String> droppedFieldNames = new java.util.LinkedHashSet<>();

        Map<Integer, LanceField> lanceFieldsById = new HashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            lanceFieldsById.put(field.getId(), field);
        }

        for (LanceField field : lanceSchema.fields()) {
            MappingFieldInfo mapped = mappingFieldIds.get(field.getId());
            if (mapped == null || mapped.name.equals(field.getName())) {
                continue;
            }
            String currentArrowType = renameArrowType(field);
            boolean typeChanged = mapped.arrowType != null && currentArrowType != null && !mapped.arrowType.equals(currentArrowType);
            if (typeChanged) {
                // Same id, different Arrow type: not a rename, the writer
                // dropped the old column and reused the id for a new one.
                String key = indexName
                    + ":reset:"
                    + field.getId()
                    + ":"
                    + mapped.name
                    + "("
                    + mapped.arrowType
                    + ")->"
                    + field.getName()
                    + "("
                    + currentArrowType
                    + ")";
                if (warnedRenamed.add(key)) {
                    LOG.warn(
                        "Lance table for {} reset field id {}: dropped '{}' ({}), added '{}' ({}). "
                            + "The mapping still exposes '{}'; marking it lance_dropped so lance_text / lance_vector queries fail. "
                            + "Standard scalar queries against '{}' still succeed silently — recreate the index to drop it.",
                        indexName,
                        field.getId(),
                        mapped.name,
                        mapped.arrowType,
                        field.getName(),
                        currentArrowType,
                        mapped.name,
                        mapped.name
                    );
                }
                droppedFieldNames.add(mapped.name);
                continue;
            }
            String key = indexName + ":rename:" + field.getId() + ":" + mapped.name + "->" + field.getName();
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} renamed field id {} from '{}' to '{}'. "
                        + "The mapping still exposes both names; marking the old name lance_dropped so lance_text / lance_vector "
                        + "queries against it fail. Standard scalar queries against '{}' still succeed silently. Recreate the "
                        + "index to drop the stale mapping.",
                    indexName,
                    field.getId(),
                    mapped.name,
                    field.getName(),
                    mapped.name
                );
            }
            droppedFieldNames.add(mapped.name);
        }
        // drop_columns / overwrite on the Lance side removes a field id
        // entirely. OpenSearch's PutMapping cannot remove properties, so
        // the stale name lingers and queries against it fail silently with
        // 0 hits (numeric doc values just return their default, term
        // queries never match). Surface the drift so operators know to
        // recreate the index.
        for (Map.Entry<Integer, MappingFieldInfo> mapped : mappingFieldIds.entrySet()) {
            int id = mapped.getKey();
            if (lanceFieldsById.containsKey(id)) {
                continue;
            }
            String staleName = mapped.getValue().name;
            String key = indexName + ":dropped:" + id + ":" + staleName;
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} no longer contains field id {} ('{}'). "
                        + "Marking the mapping field lance_dropped so lance_text / lance_vector queries against it fail. "
                        + "Standard scalar queries against '{}' still succeed silently; recreate the index to drop it.",
                    indexName,
                    id,
                    staleName,
                    staleName
                );
            }
            droppedFieldNames.add(staleName);
        }
        if (!droppedFieldNames.isEmpty()) {
            markFieldsDropped(indexName, droppedFieldNames, mappingFieldIds);
        }
    }

    /**
     * Best-effort encoding of a Lance field's Arrow type that matches the
     * strings stored under {@code meta.lance_arrow_type} at derivation
     * time. Returns {@code null} when the type is not one the deriver
     * fingerprints (unmapped columns).
     */
    private static String renameArrowType(LanceField field) {
        ArrowType type = field.getType();
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList fsl) {
            org.apache.arrow.vector.types.pojo.Field arrow = field.asArrowField();
            ArrowType child = arrow.getChildren().isEmpty() ? null : arrow.getChildren().get(0).getType();
            if (child instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fp
                && fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                return "fixed_size_list<float32>[" + fsl.getListSize() + "]";
            }
            return null;
        }
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.List) {
            if (field.getChildren().size() == 1
                && field.getChildren().get(0).getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
                return "list<utf8>";
            }
            return null;
        }
        return type.toString();
    }

    /**
     * Persist {@code meta.lance_dropped = "true"} on each supplied field
     * name via PutMapping. Silently no-ops when the underlying mapping
     * update fails — the WARN log entries in the caller are the source of
     * truth, this is a best-effort assist so custom Lance mappers can
     * reject queries at the query builder layer.
     */
    private void markFieldsDropped(String indexName, Set<String> droppedNames, Map<Integer, MappingFieldInfo> mappingFieldIds) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
            // Extra map from name → info so we can preserve type / meta
            // when re-emitting each field.
            Map<String, MappingFieldInfo> byName = new HashMap<>(mappingFieldIds.size());
            for (MappingFieldInfo info : mappingFieldIds.values()) {
                byName.put(info.name, info);
            }
            for (String name : droppedNames) {
                MappingFieldInfo info = byName.get(name);
                if (info == null) {
                    continue;
                }
                // PutMapping requires "type" to be present when updating an
                // existing field's meta, otherwise the whole field is
                // rejected. Emit the existing type and the merged meta.
                builder.startObject(name);
                if (info.osType != null) {
                    builder.field("type", info.osType);
                }
                if (info.opts != null) {
                    for (Map.Entry<String, Object> opt : info.opts.entrySet()) {
                        builder.field(opt.getKey(), opt.getValue());
                    }
                }
                builder.startObject("meta");
                builder.field("lance_field_id", Integer.toString(info.fieldId));
                if (info.arrowType != null) {
                    builder.field("lance_arrow_type", info.arrowType);
                }
                builder.field("lance_dropped", "true");
                builder.endObject();
                builder.endObject();
            }
            builder.endObject().endObject();
            client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setSource(builder.toString(), MediaTypeRegistry.JSON)
                .execute()
                .actionGet();
        } catch (Exception e) {
            LOG.debug("could not update lance_dropped meta on {}: {}", indexName, e.getMessage());
        }
    }

    /**
     * Read Lance-related meta off every top-level field in the current
     * mapping. Fields without {@code meta.lance_field_id} (older indexes,
     * non-Lance mappings) are skipped. Returns a map from Lance field id
     * to the field's OpenSearch name, type, Arrow type identifier, and
     * remaining top-level options (so a subsequent update can round-trip
     * the field unchanged).
     */
    private Map<Integer, MappingFieldInfo> readMappingFieldIds(String indexName) {
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
        Map<Integer, MappingFieldInfo> result = new HashMap<>();
        for (Map.Entry<?, ?> entry : propsMap.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> field)) {
                continue;
            }
            Object meta = field.get("meta");
            if (!(meta instanceof Map<?, ?> metaMap)) {
                continue;
            }
            Object rawId = metaMap.get("lance_field_id");
            if (!(rawId instanceof String s)) {
                continue;
            }
            int fieldId;
            try {
                fieldId = Integer.parseInt(s);
            } catch (NumberFormatException e) {
                // A hand-crafted mapping may put anything here; skip malformed entries.
                continue;
            }
            String osType = field.get("type") instanceof String t ? t : null;
            String arrowType = metaMap.get("lance_arrow_type") instanceof String at ? at : null;
            // Preserve any per-type options (e.g. lance_vector's dimension /
            // element_type) so a subsequent PutMapping to update meta round
            // trips the field unchanged.
            Map<String, Object> opts = new HashMap<>();
            for (Map.Entry<?, ?> optEntry : field.entrySet()) {
                if (!(optEntry.getKey() instanceof String optName)) {
                    continue;
                }
                if ("type".equals(optName) || "meta".equals(optName)) {
                    continue;
                }
                opts.put(optName, optEntry.getValue());
            }
            result.put(fieldId, new MappingFieldInfo(fieldId, name, osType, arrowType, opts));
        }
        return result;
    }

    /**
     * Snapshot of one top-level mapping field, kept around so drift
     * handling and lance_dropped updates can round-trip the field without
     * losing type-specific options.
     */
    private record MappingFieldInfo(int fieldId, String name, String osType, String arrowType, Map<String, Object> opts) {
    }

    private String readUncoveredFragmentPolicy(String indexName) {
        try {
            var state = client.admin().cluster().prepareState().execute().actionGet().getState();
            var metadata = state.metadata().index(indexName);
            if (metadata == null) {
                return "immediate";
            }
            return metadata.getSettings().get("index.lance.uncovered_fragment_policy", "immediate");
        } catch (Exception e) {
            LOG.warn("failed to read uncovered_fragment_policy for {}: {}", indexName, e.getMessage());
            return "immediate";
        }
    }

    private void warnDeprecatedWaitPolicyOnce(String indexName) {
        if (warnedWaitPolicy.add(indexName)) {
            LOG.info(
                "index [{}] has index.lance.uncovered_fragment_policy=wait, but the plugin no longer runs auto-optimize on the user's Lance table. "
                    + "Index maintenance is expected to happen outside OpenSearch (Python, Ray, Spark, or the Lance Java SDK) or via "
                    + "an explicit POST /_lance/build_indexes/{{index}} call. The wait value is accepted for a future async-optimize implementation.",
                indexName
            );
        }
    }

    private record RegisteredNamespace(String rootUri, DirectoryNamespace namespace, StorageOptions storageOptions) {
    }

    private record AttachedIndex(String tablePath, StorageOptions storageOptions) {
    }
}
