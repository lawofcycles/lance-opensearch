/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Builds the planner's view of one Lance backed index: the
 * {@link LanceTable} holding its Arrow schema and row count, the
 * {@link LanceSchema} the planner resolves the index name against, and
 * the multi-fields spec the translator needs to resolve keyword
 * sub-fields to their base column.
 *
 * <p>{@link #build} reads everything else from the cluster state's
 * {@link IndexMetadata} (table URI, storage options, pinned version or
 * tag, primary key, multi-fields) and reaches the table through the
 * node's {@link LanceWarmCache}, the same snapshot cache the fragment
 * executors acquire from, so a plan construction reuses an open
 * {@code Dataset} when the node has one and shares the node's Lance
 * {@code Session} when it does not. The Arrow schema and the row count
 * (sum of {@code Dataset.getFragmentStatistics()} row counts) are read
 * once while the lease is held; the model keeps no reference to the
 * dataset.
 */
public final class LanceSchemas {

    private LanceSchemas() {}

    /**
     * The planner inputs for one index: the name plans scan by, the
     * Arrow schema and multi-fields spec field resolution reads, the
     * renames the mapping records (stale column name to the name the
     * Lance table uses now, so field resolution can refuse a stale name
     * with the rename instead of a generic unknown-field message), and
     * the table / schema pair the {@code RelBuilder} resolves against.
     * {@code multiFields} maps a base column to its declared sub-fields
     * ({@code body -> {raw: keyword}}), empty when the attach declared
     * none.
     */
    public record IndexModel(String indexName, Schema arrowSchema, Map<String, LinkedHashMap<String, String>> multiFields, Map<
        String,
        String> renamedFields, LanceTable table, LanceSchema schema) {
    }

    /**
     * A consistent model from its parts; tests pass fixtures, and
     * {@link #build} passes what it read from the open dataset.
     */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        LongSupplier rowCount
    ) {
        return model(indexName, arrowSchema, multiFields, Map.of(), rowCount);
    }

    /** The model with the mapping's recorded renames (stale name to live name). */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        LongSupplier rowCount
    ) {
        LanceTable table = new LanceTable(indexName, arrowSchema, rowCount);
        return new IndexModel(indexName, arrowSchema, multiFields, renamedFields, table, new LanceSchema(Map.of(indexName, table)));
    }

    /**
     * The model for a Lance backed index, read through the node's warm
     * cache. The lease is released before returning: the Arrow schema
     * is a plain Java object once read and the row count is captured as
     * a constant, so nothing of the model outlives the snapshot.
     *
     * <p>The primary key and multi-fields arguments of the acquire
     * mirror what the fragment executor passes, because a snapshot this
     * call builds on a cold node is keyed on (index UUID, version) and
     * later serves search requests.
     *
     * @param indexMetadata cluster state metadata of a Lance backed
     *     index; the caller has already checked {@code index.lance.table}
     *     is present
     * @param warmCache the node's snapshot cache
     */
    public static IndexModel build(IndexMetadata indexMetadata, LanceWarmCache warmCache) throws IOException {
        String indexName = indexMetadata.getIndex().getName();
        Settings settings = indexMetadata.getSettings();
        String tableUri = settings.get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        long pinnedVersion = settings.getAsLong(LanceEngineFactory.VERSION_SETTING, -1L);
        String tag = settings.get(LanceEngineFactory.TAG_SETTING, "");
        if (pinnedVersion < 0 && !tag.isEmpty()) {
            pinnedVersion = LanceRegistry.resolveTagVersion(tableUri, storageOptions, tag);
        }
        Optional<Long> version = pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
        String pkField = settings.get(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, "");
        LancePrimaryKeyType pkType = pkField.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(settings.get(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, "long"));
        LanceOverrides overrides = LanceOverrides.of(settings);
        Map<String, LinkedHashMap<String, String>> multiFields = overrides.subFields();
        Map<String, String> renamedFields = new LinkedHashMap<>();
        for (LanceMappingMeta.RenamedField renamed : LanceMappingMeta.renamedFields(indexMetadata.mapping())) {
            renamedFields.put(renamed.from(), renamed.to());
        }
        try (
            LanceWarmCache.Lease lease = warmCache.acquire(
                indexMetadata.getIndexUUID(),
                tableUri,
                storageOptions,
                version,
                pkField,
                pkType,
                overrides
            )
        ) {
            Dataset dataset = lease.snapshot().dataset();
            Schema arrowSchema = dataset.getSchema();
            long rows = 0L;
            for (long fragmentRows : dataset.getFragmentStatistics().getRowCounts()) {
                rows += fragmentRows;
            }
            final long total = rows;
            return model(indexName, arrowSchema, multiFields, renamedFields, () -> total);
        }
    }
}
