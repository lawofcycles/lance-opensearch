/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.transport.client.Client;

/**
 * Detection of drift between a Lance table's current schema and the
 * OpenSearch mapping derived earlier: column renames, schema
 * resets (a field id reused with a different Arrow type) and drops.
 * Warns once per observation, marks stale names as
 * {@code lance_dropped} in the mapping's field meta, and follows the
 * operator's mapping overrides across the drift before the mapping is
 * re-derived.
 */
final class LanceSchemaDriftDetector {

    private static final Logger LOG = LogManager.getLogger(LanceSchemaDriftDetector.class);

    private final Client client;
    /**
     * Run once for every mapping read or mapping update the detection
     * could not complete. Such a failure leaves the drift check for
     * that version undone (a rename or drop is not marked, an override
     * is not moved) while the freshness check proceeds, so the owner
     * counts it among the check's failures.
     */
    private final Runnable onFailure;
    // Track rename warnings so a table that renamed the same field is not
    // logged on every check. Keyed by "indexName:fieldId:oldName->newName" so
    // the same rename fires once, but a later re-rename still warns.
    private final Set<String> warnedRenamed = ConcurrentHashMap.newKeySet();

    LanceSchemaDriftDetector(Client client, Runnable onFailure) {
        this.client = client;
        this.onFailure = onFailure;
    }

    /**
     * Drop the warned-once bookkeeping recorded under exactly
     * {@code indexName}. Called when the index leaves cluster state.
     */
    void forgetIndex(String indexName) {
        warnedRenamed.remove(indexName);
    }

    /**
     * Follow the operator's mapping overrides across schema drift before
     * the mapping is re-derived. Two moves:
     * <ul>
     *   <li>Rename (a mapping field id now carries a different name in
     *       the Lance schema): every override keyed by the old column
     *       name is re-keyed to the new name, so the operator's
     *       {@code type} / {@code format} / {@code fields} rules follow
     *       the column and the re-derivation applies them to the new
     *       name.</li>
     *   <li>Reset (a column's Arrow type changed): the override is
     *       checked against the new type. A still-valid override stays
     *       ({@code type: date} on a column recast from Int64 to
     *       Timestamp); an override the new type does not admit is
     *       dropped from the setting with one warning naming the
     *       column, the override and the new type, so the setting does
     *       not carry a rule that can never apply again.</li>
     * </ul>
     * An override whose column is absent from the schema entirely is
     * left in the setting, as ever: it waits for a manifest that
     * restores the column.
     *
     * <p>The rewritten JSON is persisted with an update-settings call on
     * {@code index.lance.overrides} (Dynamic for exactly this purpose).
     * When persisting fails the stored overrides are returned unchanged
     * and the rewrite retries on the next check.
     */
    LanceOverrides rewriteOverridesForSchemaDrift(String indexName, LanceOverrides stored, LanceSchema lanceSchema) {
        if (stored.isEmpty()) {
            return stored;
        }
        Map<Integer, MappingFieldInfo> mappingFieldIds;
        try {
            mappingFieldIds = readMappingFieldIds(indexName);
        } catch (Exception e) {
            onFailure.run();
            LOG.warn(
                "could not inspect mapping meta for {}; overrides are not followed across schema drift this check: {}",
                indexName,
                e.getMessage()
            );
            return stored;
        }
        Map<String, LanceField> lanceFieldsByName = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            lanceFieldsByName.put(field.getName(), field);
        }
        // Old name -> new name for every field id whose name moved.
        // A reset (different Arrow type under the same id) re-keys too:
        // the compatibility check below decides whether the override
        // survives on the new name.
        Map<String, String> renames = new LinkedHashMap<>();
        for (LanceField field : lanceSchema.fields()) {
            MappingFieldInfo mapped = mappingFieldIds.get(field.getId());
            if (mapped != null && !mapped.name.equals(field.getName())) {
                renames.put(mapped.name, field.getName());
            }
        }
        LanceOverrides rewritten = stored.withRenamedColumns(renames);
        // Compatibility: a column present in the schema must still admit
        // its override. Covers in-place type changes (same name, new
        // Arrow type) and renamed-plus-reset ids alike.
        for (Map.Entry<String, LanceOverrides.Column> entry : new LinkedHashMap<>(rewritten.columns()).entrySet()) {
            LanceField field = lanceFieldsByName.get(entry.getKey());
            if (field == null) {
                continue;
            }
            try {
                RestAttachAction.validateColumnOverride(entry.getKey(), entry.getValue(), field, lanceFieldsByName.keySet());
            } catch (IllegalArgumentException e) {
                rewritten = rewritten.withoutColumn(entry.getKey());
                String key = indexName + ":override-drop:" + entry.getKey() + ":" + field.getType();
                if (warnedRenamed.add(key)) {
                    LOG.warn(
                        "dropping mapping override on column '{}' of {}: the Lance schema reset the column to {} and the "
                            + "override no longer applies ({})",
                        entry.getKey(),
                        indexName,
                        field.getType(),
                        e.getMessage()
                    );
                }
            }
        }
        if (rewritten.equals(stored)) {
            return stored;
        }
        try {
            client.admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(LanceEngineFactory.OVERRIDES_SETTING, rewritten.toJson()).build())
                .execute()
                .actionGet();
            LOG.info("rewrote index.lance.overrides of {} after a Lance schema change: {}", indexName, rewritten.toJson());
            return rewritten;
        } catch (Exception e) {
            onFailure.run();
            LOG.warn("could not persist rewritten overrides for {}: {}", indexName, e.getMessage());
            return stored;
        }
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
     * {@code lance_dropped} is stored in the field's {@code meta} so the
     * read paths can hide the stale name: {@code LanceTextFieldMapper},
     * {@code LanceVectorFieldMapper} and the Lance query builders reject
     * queries against it up front, the coordinator's field type lookup
     * treats it as unmapped (no Lance SQL ever names the stale column,
     * scalar queries fold to 0 hits), and the explain endpoint's field
     * resolution names the rename. {@code GET _mapping} still lists the
     * stale name because PutMapping cannot remove properties.
     */
    void warnOnLanceFieldRename(String indexName, LanceSchema lanceSchema) {
        Map<Integer, MappingFieldInfo> mappingFieldIds;
        try {
            mappingFieldIds = readMappingFieldIds(indexName);
        } catch (Exception e) {
            onFailure.run();
            LOG.warn("could not inspect mapping meta for {}; schema drift is not detected this check: {}", indexName, e.getMessage());
            return;
        }
        if (mappingFieldIds.isEmpty()) {
            return;
        }
        // Collect field names to mark as dropped after the loop so we can
        // issue a single PutMapping call. Empty when no drift is observed.
        Set<String> droppedFieldNames = new LinkedHashSet<>();

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
                            + "The mapping still exposes '{}' marked lance_dropped: queries against it return no hits and "
                            + "lance_text / lance_vector queries fail. Recreate the index to drop it from the mapping.",
                        indexName,
                        field.getId(),
                        mapped.name,
                        mapped.arrowType,
                        field.getName(),
                        currentArrowType,
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
                        + "The mapping keeps the old name marked lance_dropped (PutMapping cannot remove properties): queries "
                        + "against '{}' return no hits, and GET /_lance/stats lists the rename under renamed_fields. "
                        + "Mapping overrides keyed by the old name follow the column to '{}'.",
                    indexName,
                    field.getId(),
                    mapped.name,
                    field.getName(),
                    mapped.name,
                    field.getName()
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
                        + "The mapping keeps the name marked lance_dropped (PutMapping cannot remove properties): queries "
                        + "against '{}' return no hits. Recreate the index to drop it from the mapping.",
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
        if (type instanceof ArrowType.FixedSizeList fsl) {
            Field arrow = field.asArrowField();
            ArrowType child = arrow.getChildren().isEmpty() ? null : arrow.getChildren().get(0).getType();
            if (child instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                return "fixed_size_list<float32>[" + fsl.getListSize() + "]";
            }
            return null;
        }
        if (type instanceof ArrowType.List) {
            if (field.getChildren().size() == 1 && field.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
                return "list<utf8>";
            }
            return null;
        }
        return type.toString();
    }

    /**
     * Persist {@code meta.lance_dropped = "true"} on each supplied field
     * name via PutMapping. When the mapping update fails the stale names
     * stay unmarked until a later check succeeds: the WARN entries the
     * caller logged name the drift, and the failure is counted, but the
     * custom Lance mappers cannot reject queries against the stale names
     * at the query builder layer in the meantime.
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
            onFailure.run();
            LOG.warn("could not mark {} as lance_dropped in the mapping of {}: {}", droppedNames, indexName, e.getMessage());
        }
    }

    /**
     * Read Lance-related meta off every top-level field in the current
     * mapping. Fields without {@code meta.lance_field_id} (older indexes,
     * non-Lance mappings) are skipped, and so are fields already marked
     * {@code lance_dropped}: after a rename both the stale and the live
     * name carry the same field id, and drift detection must see the
     * live one only, or every later check would re-detect the rename the
     * mapping already recorded. Returns a map from Lance field id
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
            if ("true".equals(metaMap.get("lance_dropped"))) {
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
}
