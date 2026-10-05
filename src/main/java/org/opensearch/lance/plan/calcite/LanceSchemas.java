/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.metadata.TableStatistics;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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
 * {@code Session} when it does not. The Arrow schema is read while the
 * lease is held and the {@link TableStatistics} come from the node's
 * statistics cache under the snapshot's version (collected in the
 * background on the first request of that version, which plans
 * without them); the model keeps no reference to the dataset.
 */
public final class LanceSchemas {

    private static final Logger LOGGER = LogManager.getLogger(LanceSchemas.class);

    private LanceSchemas() {}

    /**
     * The planner inputs for one index: the name plans scan by, the
     * Arrow schema and multi-fields spec field resolution reads, the
     * renames the mapping records (stale column name to the name the
     * Lance table uses now, so field resolution can refuse a stale name
     * with the rename instead of a generic unknown-field message), the
     * primary key column {@code ids} queries resolve against (empty
     * when the index declared none), the integer columns the attach
     * overrode as {@code date} (their epoch-millis literals accept the
     * ISO-8601 strings the override serves), and the table / schema
     * pair the {@code RelBuilder} resolves
     * against. {@code multiFields} maps a base column to its declared
     * sub-fields ({@code body -> {raw: keyword}}), empty when the attach
     * declared none.
     */
    public record IndexModel(String indexName, Schema arrowSchema, Map<String, LinkedHashMap<String, String>> multiFields, Map<
        String,
        String> renamedFields, String primaryKeyField, Set<String> dateOverrideColumns, LanceTable table, LanceSchema schema) {
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
        return model(indexName, arrowSchema, multiFields, Map.of(), "", Set.of(), rowCount);
    }

    /** The model with the mapping's recorded renames (stale name to live name). */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        LongSupplier rowCount
    ) {
        return model(indexName, arrowSchema, multiFields, renamedFields, "", Set.of(), rowCount);
    }

    /**
     * The {@link #model(String, Schema, Map, LongSupplier)} shape with a
     * primary key column, for indexes whose attach declared one.
     */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        String primaryKeyField,
        LongSupplier rowCount
    ) {
        return model(indexName, arrowSchema, multiFields, Map.of(), primaryKeyField, Set.of(), rowCount);
    }

    /** The model with the recorded renames, the primary key column and the date override columns, as {@link #build} reads them. */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        String primaryKeyField,
        Set<String> dateOverrideColumns,
        LongSupplier rowCount
    ) {
        return model(indexName, arrowSchema, multiFields, renamedFields, primaryKeyField, dateOverrideColumns, rowCount, null, null);
    }

    /**
     * The model over the table's collected {@link TableStatistics}: the
     * table's row count is the statistics' live row count and the
     * planner's metadata handlers read the column statistics from them.
     */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        String primaryKeyField,
        Set<String> dateOverrideColumns,
        TableStatistics statistics
    ) {
        return model(indexName, arrowSchema, multiFields, renamedFields, primaryKeyField, dateOverrideColumns, statistics, null);
    }

    /**
     * {@link #model(String, Schema, Map, Map, String, Set, TableStatistics)}
     * with the Utf8 columns that carry an inverted index
     * ({@code ftsIndexedColumns}, the snapshot's; null when unknown), so
     * a pushed full text match can say whether each searched column is
     * answered from the index or by Lance's flat scan.
     */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        String primaryKeyField,
        Set<String> dateOverrideColumns,
        TableStatistics statistics,
        Set<String> ftsIndexedColumns
    ) {
        return model(
            indexName,
            arrowSchema,
            multiFields,
            renamedFields,
            primaryKeyField,
            dateOverrideColumns,
            statistics::rowCount,
            () -> statistics,
            ftsIndexedColumns
        );
    }

    /**
     * {@link #model(String, Schema, Map, Map, String, Set, LongSupplier)}
     * with the Utf8 columns that carry an inverted index
     * ({@code ftsIndexedColumns}, the snapshot's; null when unknown).
     */
    public static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        String primaryKeyField,
        Set<String> dateOverrideColumns,
        LongSupplier rowCount,
        Set<String> ftsIndexedColumns
    ) {
        return model(
            indexName,
            arrowSchema,
            multiFields,
            renamedFields,
            primaryKeyField,
            dateOverrideColumns,
            rowCount,
            null,
            ftsIndexedColumns
        );
    }

    private static IndexModel model(
        String indexName,
        Schema arrowSchema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Map<String, String> renamedFields,
        String primaryKeyField,
        Set<String> dateOverrideColumns,
        LongSupplier rowCount,
        Supplier<TableStatistics> statistics,
        Set<String> ftsIndexedColumns
    ) {
        LanceTable table = new LanceTable(indexName, arrowSchema, rowCount, statistics, ftsIndexedColumns);
        return new IndexModel(
            indexName,
            arrowSchema,
            multiFields,
            renamedFields,
            primaryKeyField,
            dateOverrideColumns,
            table,
            new LanceSchema(Map.of(indexName, table))
        );
    }

    /**
     * The model for a Lance backed index, read through the node's warm
     * cache. The lease is released before returning: the Arrow schema
     * is a plain Java object once read and the statistics are an
     * immutable object owned by the statistics cache, so nothing of
     * the model outlives the snapshot.
     *
     * <p>The primary key and multi-fields arguments of the acquire
     * mirror what the fragment executor passes, because a snapshot this
     * call builds on a cold node is keyed on (index UUID, version) and
     * later serves search requests.
     *
     * @param indexMetadata cluster state metadata of a Lance backed
     *     index; the caller has already checked {@code index.plugins.lance.table}
     *     is present
     * @param warmCache the node's snapshot cache
     */
    public static IndexModel build(IndexMetadata indexMetadata, LanceWarmCache warmCache) throws IOException {
        return build(indexMetadata, warmCache, Set.of());
    }

    /**
     * {@link #build(IndexMetadata, LanceWarmCache)}, reading as well the
     * zone maps of the columns {@code queryFields} name while the lease
     * is held ({@link TableStatistics#readZoneMaps}), so the plan built
     * over the model can exclude the fragments the query predicate
     * cannot match. A zone map that fails to read is logged and left
     * unread: pruning is an input to plan quality, not to correctness.
     *
     * @param queryFields the fields the query's leaves name
     *     ({@link org.opensearch.lance.plan.translate.QueryToRex#referencedFields})
     */
    public static IndexModel build(IndexMetadata indexMetadata, LanceWarmCache warmCache, Set<String> queryFields) throws IOException {
        String indexName = indexMetadata.getIndex().getName();
        Settings settings = indexMetadata.getSettings();
        String tableUri = LanceEngineFactory.tableOf(settings);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        long pinnedVersion = LanceSettings.VERSION_SETTING.get(settings);
        String tag = LanceSettings.TAG_SETTING.get(settings);
        if (pinnedVersion < 0 && !tag.isEmpty()) {
            pinnedVersion = LanceRegistry.resolveTagVersion(tableUri, storageOptions, tag);
        }
        Optional<Long> version = pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
        String pkField = LanceSettings.PRIMARY_KEY_FIELD_SETTING.get(settings);
        LancePrimaryKeyType pkType = pkField.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(LanceSettings.PRIMARY_KEY_TYPE_SETTING.get(settings));
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
            long snapshotVersion = lease.snapshot().version();
            // The columns whose inverted index the snapshot resolved when
            // it opened, the same set the executors' admission gate
            // splits a full text clause by; the pushed match reports
            // them in the explain output.
            Set<String> ftsIndexedColumns = lease.snapshot().ftsColumns();
            // The collection the cache starts on a miss opens the table
            // on its own: the snapshot's dataset closes with the
            // snapshot.
            TableStatistics statistics = warmCache.tableStatistics()
                .lookup(dataset.uri(), snapshotVersion, () -> LanceRegistry.openDatasetAt(tableUri, storageOptions, snapshotVersion));
            if (statistics != null) {
                try {
                    statistics.readZoneMaps(dataset, queryFields);
                } catch (RuntimeException e) {
                    LOGGER.warn("zone maps of [{}] unavailable, planning without pruning", indexName, e);
                }
                return model(
                    indexName,
                    arrowSchema,
                    multiFields,
                    renamedFields,
                    pkField,
                    overrides.dateColumns().keySet(),
                    statistics,
                    ftsIndexedColumns
                );
            }
            // The statistics are being collected in the background:
            // this plan reads the fragment row counts instead.
            LOGGER.debug("table statistics of [{}] at version {} not collected yet, planning without them", indexName, snapshotVersion);
            long rows = 0L;
            for (long fragmentRows : dataset.getFragmentStatistics().getRowCounts()) {
                rows += fragmentRows;
            }
            final long total = rows;
            return model(
                indexName,
                arrowSchema,
                multiFields,
                renamedFields,
                pkField,
                overrides.dateColumns().keySet(),
                () -> total,
                ftsIndexedColumns
            );
        }
    }
}
