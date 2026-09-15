/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.OptimizeOptions;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.vector.VectorIndexParams;
import org.opensearch.lance.rest.RestAttachAction;

/**
 * The RFC's index builder role, minimal form. Builds the FTS, scalar and
 * vector indexes over the columns picked by {@link RestAttachAction}#derive,
 * committed back to the table with CreateIndex transactions.
 *
 * Load control (design-notes.md decision 33). When {@code dataset.countRows()}
 * exceeds {@code maxRows}, every auto-build is skipped and logged as WARN.
 * IVF_PQ has an additional lower bound of 256 rows for PQ training; smaller
 * tables skip the vector build.
 *
 * Per-fragment build (design-notes.md decision 33). Every method accepts an
 * {@code Optional<List<Integer>> fragmentIds}. When present the build is
 * restricted to those fragments via {@link IndexOptions.Builder#withFragmentIds}.
 * When absent the build spans every fragment. FTS and scalar have no training
 * step and simply add fragments to any existing index; the vector path
 * detects an existing index and switches {@code train=true} (initial build)
 * or {@code train=false} (extend with existing centroids and codebook).
 *
 * Recovery. Automatic paths (surface / syncTable) pass the node-level
 * {@code lance.builder.max_rows}; the manual endpoint passes
 * {@code Long.MAX_VALUE} so operators can force a build on tables the auto
 * path skipped. Skip conditions and recovery are documented in decision 33.
 */
public final class LanceIndexBuilder {

    private static final Logger LOG = LogManager.getLogger(LanceIndexBuilder.class);
    private static final long IVF_PQ_MIN_ROWS = 256L;

    private LanceIndexBuilder() {}

    /**
     * Builds FTS indexes for the given target columns. Columns already carrying
     * an FTS index are skipped unless {@code fragmentIds} is present, in which
     * case the specified fragments are added to the existing index. Returns
     * the columns that received a CreateIndex commit.
     */
    public static List<String> ensureFtsIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds
    ) {
        if (targetColumns.isEmpty()) {
            return Collections.emptyList();
        }
        if (exceedsMaxRows(dataset, maxRows, "FTS", targetColumns)) {
            return Collections.emptyList();
        }
        List<String> built = new ArrayList<>();
        for (Field field : dataset.getSchema().getFields()) {
            if (!(field.getType() instanceof ArrowType.Utf8)) {
                continue;
            }
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            try {
                boolean hasFts = !dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).mustSupportFts(true).build())
                    .isEmpty();
                if (hasFts && fragmentIds.isEmpty()) {
                    continue;
                }
                if (hasFts && fragmentIds.isPresent()) {
                    LOG.warn("FTS index for column {} exists; use optimize=true to add fragments, skipping partial build", column);
                    continue;
                }
                ScalarIndexParams fts = ScalarIndexParams.create("inverted", "{\"base_tokenizer\":\"simple\"}");
                String indexName = column + "_fts";
                IndexOptions.Builder builder = IndexOptions.builder(
                    Collections.singletonList(column),
                    IndexType.INVERTED,
                    IndexParams.builder().setScalarIndexParams(fts).build()
                ).withIndexName(indexName);
                fragmentIds.ifPresent(builder::withFragmentIds);
                Index segment = dataset.createIndex(builder.build());
                if (fragmentIds.isPresent()) {
                    dataset.commitExistingIndexSegments(indexName, column, Collections.singletonList(segment));
                }
                built.add(column);
                LOG.info(
                    "built FTS index over column {} (fragmentIds={}, version {})",
                    column,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("FTS build failed for column {}", column, e);
            }
        }
        return built;
    }

    /**
     * Builds BTree scalar indexes for numeric / date / boolean / keyword columns.
     * Columns already carrying a scalar index are skipped unless
     * {@code fragmentIds} is present.
     */
    public static List<String> ensureScalarIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds
    ) {
        if (targetColumns.isEmpty()) {
            return Collections.emptyList();
        }
        if (exceedsMaxRows(dataset, maxRows, "scalar", targetColumns)) {
            return Collections.emptyList();
        }
        List<String> built = new ArrayList<>();
        for (Field field : dataset.getSchema().getFields()) {
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            try {
                boolean hasScalar = hasScalarIndex(dataset, column);
                if (hasScalar && fragmentIds.isEmpty()) {
                    continue;
                }
                if (hasScalar && fragmentIds.isPresent()) {
                    LOG.warn("scalar index for column {} exists; use optimize=true to add fragments, skipping partial build", column);
                    continue;
                }
                ScalarIndexParams btree = ScalarIndexParams.create("btree");
                String indexName = column + "_btree";
                IndexOptions.Builder builder = IndexOptions.builder(
                    Collections.singletonList(column),
                    IndexType.BTREE,
                    IndexParams.builder().setScalarIndexParams(btree).build()
                ).withIndexName(indexName);
                fragmentIds.ifPresent(builder::withFragmentIds);
                Index segment = dataset.createIndex(builder.build());
                if (fragmentIds.isPresent()) {
                    dataset.commitExistingIndexSegments(indexName, column, Collections.singletonList(segment));
                }
                built.add(column);
                LOG.info(
                    "built BTree scalar index over column {} (fragmentIds={}, version {})",
                    column,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("scalar build failed for column {}", column, e);
            }
        }
        return built;
    }

    /**
     * Builds IVF_PQ vector indexes for FixedSizeList&lt;float&gt; columns.
     * Skips when {@code dataset.countRows()} is below the IVF_PQ training
     * minimum (256). Detects an existing index and switches
     * {@code train=true} (initial build) or {@code train=false} (extend with
     * existing centroids and codebook).
     */
    public static List<String> ensureVectorIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds
    ) {
        if (targetColumns.isEmpty()) {
            return Collections.emptyList();
        }
        if (exceedsMaxRows(dataset, maxRows, "vector", targetColumns)) {
            return Collections.emptyList();
        }
        long rows;
        try {
            rows = dataset.countRows();
        } catch (Exception e) {
            LOG.warn("countRows failed for vector build, skipping", e);
            return Collections.emptyList();
        }
        if (rows < IVF_PQ_MIN_ROWS) {
            LOG.warn("skipping vector build over {}: table has {} rows, below IVF_PQ minimum {}", targetColumns, rows, IVF_PQ_MIN_ROWS);
            return Collections.emptyList();
        }
        List<String> built = new ArrayList<>();
        for (Field field : dataset.getSchema().getFields()) {
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            try {
                boolean hasVector = hasVectorIndex(dataset, column);
                if (hasVector && fragmentIds.isEmpty()) {
                    continue;
                }
                if (hasVector && fragmentIds.isPresent()) {
                    LOG.warn("vector index for column {} exists; use optimize=true to add fragments, skipping partial build", column);
                    continue;
                }
                // Initial build only in this method. train=true because we only
                // reach this branch when the index does not yet exist. Adding
                // fragments to an existing vector index goes through
                // {@link #optimizeIndexes}.
                VectorIndexParams ivfPq = VectorIndexParams.ivfPq(1, 8, 8, DistanceType.L2, 20);
                String indexName = column + "_vec";
                IndexOptions.Builder builder = IndexOptions.builder(
                    Collections.singletonList(column),
                    IndexType.IVF_PQ,
                    IndexParams.builder().setVectorIndexParams(ivfPq).build()
                ).withIndexName(indexName).train(true);
                fragmentIds.ifPresent(builder::withFragmentIds);
                Index segment = dataset.createIndex(builder.build());
                if (fragmentIds.isPresent()) {
                    dataset.commitExistingIndexSegments(indexName, column, Collections.singletonList(segment));
                }
                built.add(column);
                LOG.info(
                    "built IVF_PQ vector index over column {} (fragmentIds={}, version {})",
                    column,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("vector build failed for column {}", column, e);
            }
        }
        return built;
    }

    private static boolean exceedsMaxRows(Dataset dataset, long maxRows, String kind, Set<String> targetColumns) {
        long rows;
        try {
            rows = dataset.countRows();
        } catch (Exception e) {
            LOG.warn("countRows failed for {} build over {}, skipping", kind, targetColumns, e);
            return true;
        }
        if (rows > maxRows) {
            LOG.warn("skipping {} build over {}: table has {} rows, exceeds lance.builder.max_rows={}", kind, targetColumns, rows, maxRows);
            return true;
        }
        return false;
    }

    /**
     * Optimizes existing indexes via {@link Dataset#optimizeIndices}. Runs the
     * Lance incremental merge that picks up fragments not yet covered by the
     * named indexes. When {@code retrain} is true the indexes are rebuilt from
     * scratch (vector codebook and centroids relearn as well). Returns the
     * index names that were optimized. Lance's OptimizeOptions does not accept
     * fragment ids; the merge covers every out-of-index fragment automatically.
     */
    public static List<String> optimizeIndexes(Dataset dataset, List<String> indexNames, boolean retrain) {
        if (indexNames.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            dataset.optimizeIndices(OptimizeOptions.builder().indexNames(indexNames).retrain(retrain).build());
            LOG.info("optimized indexes {} (retrain={}, version {})", indexNames, retrain, dataset.version());
            return indexNames;
        } catch (Exception e) {
            LOG.warn("optimize failed for {}", indexNames, e);
            return Collections.emptyList();
        }
    }

    /**
     * Extends the FTS index of every column in {@code columns} that already
     * carries one (through {@link Dataset#optimizeIndices}), so an appended
     * fragment is picked up before the reader sees the new manifest. Columns
     * without an FTS index are ignored; a matching {@code ensureFtsIndexes}
     * call is expected to create them separately.
     */
    public static List<String> optimizeExistingFtsIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        if (columns.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> indexNames = new java.util.ArrayList<>();
        for (String column : columns) {
            for (IndexDescription desc : dataset.describeIndices(
                new IndexCriteria.Builder().forColumn(column).mustSupportFts(true).build()
            )) {
                indexNames.add(desc.getName());
            }
        }
        return optimizeIndexes(dataset, indexNames, retrain);
    }

    /**
     * Same shape as {@link #optimizeExistingFtsIndexes(Dataset, Set, boolean)}
     * but limited to scalar index types (BTree / Bitmap / LabelList / ZoneMap /
     * NGram / BloomFilter). Vector and FTS indexes on the same column stay
     * untouched.
     */
    public static List<String> optimizeExistingScalarIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        if (columns.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> indexNames = new java.util.ArrayList<>();
        for (String column : columns) {
            for (IndexDescription desc : dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build())) {
                if (SCALAR_INDEX_TYPES.contains(normaliseIndexType(desc.getIndexType()))) {
                    indexNames.add(desc.getName());
                }
            }
        }
        return optimizeIndexes(dataset, indexNames, retrain);
    }

    /**
     * Same shape as {@link #optimizeExistingFtsIndexes(Dataset, Set, boolean)}
     * but limited to vector index types (IVF_*, Vector). Keeps the wait
     * policy honest for KNN queries after an append.
     */
    public static List<String> optimizeExistingVectorIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        if (columns.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> indexNames = new java.util.ArrayList<>();
        for (String column : columns) {
            for (IndexDescription desc : dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build())) {
                String normalised = normaliseIndexType(desc.getIndexType());
                if (normalised.startsWith("IVF") || "VECTOR".equals(normalised)) {
                    indexNames.add(desc.getName());
                }
            }
        }
        return optimizeIndexes(dataset, indexNames, retrain);
    }

    // Returns true if the column already carries any Lance scalar index type
    // (BTree, Bitmap, LabelList, ZoneMap, NGram, BloomFilter, Scalar). INVERTED
    // (FTS) is not counted here because {@link RestAttachAction}#derive routes
    // Utf8-with-FTS through ftsColumns and Utf8-without-FTS through scalarColumns,
    // so the scalar builder only sees keyword columns that never carry FTS.
    //
    // Lance's IndexDescription#getIndexType returns the Rust-side Display form
    // (e.g. "BTree", "LabelList"). Compare in a normalised form so we tolerate
    // both the Display and the shouty-snake spellings.
    private static final Set<String> SCALAR_INDEX_TYPES = Set.of(
        "BTREE",
        "BITMAP",
        "LABELLIST",
        "ZONEMAP",
        "NGRAM",
        "BLOOMFILTER",
        "SCALAR"
    );

    private static String normaliseIndexType(String type) {
        return type == null ? "" : type.replace("_", "").toUpperCase(java.util.Locale.ROOT);
    }

    private static boolean hasScalarIndex(Dataset dataset, String column) {
        List<IndexDescription> indexes = dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build());
        for (IndexDescription desc : indexes) {
            if (SCALAR_INDEX_TYPES.contains(normaliseIndexType(desc.getIndexType()))) {
                return true;
            }
        }
        return false;
    }

    // Returns true if the column already carries any Lance vector index type
    // (IVF_Flat, IVF_PQ, IVF_SQ, IVF_HNSW_SQ, IVF_HNSW_PQ, IVF_HNSW_FLAT,
    // IVF_RQ, Vector). Uses the same normalisation as the scalar check.
    private static boolean hasVectorIndex(Dataset dataset, String column) {
        List<IndexDescription> indexes = dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build());
        for (IndexDescription desc : indexes) {
            String normalised = normaliseIndexType(desc.getIndexType());
            if (normalised.startsWith("IVF") || "VECTOR".equals(normalised)) {
                return true;
            }
        }
        return false;
    }
}
