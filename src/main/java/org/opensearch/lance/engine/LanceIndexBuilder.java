/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;

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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.rest.RestAttachAction;

/**
 * The RFC's index builder role, minimal form. Builds the FTS, scalar and
 * vector indexes over the columns picked by {@link RestAttachAction}#derive,
 * committed back to the table with CreateIndex transactions.
 *
 * <p>Every public method reports per column through a {@link BuildResult}:
 * {@link BuildResult#built()} lists what received a CreateIndex (or
 * OptimizeIndices) commit, {@link BuildResult#skipped()} lists columns the
 * builder decided not to touch with the reason (index already present, too
 * few rows, table over {@code maxRows}), and {@link BuildResult#failed()}
 * lists columns where Lance threw, with the exception. Nothing is swallowed:
 * an I/O error from {@link Dataset#createIndex} ends up in {@code failed}
 * so the caller can answer with an error status instead of a 200 whose
 * {@code built} list happens to be empty.
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

    /**
     * Lance {@code base_tokenizer} used for FTS indexes when the caller does
     * not name one. Splits on whitespace and punctuation, which is what the
     * plugin always did before the tokenizer became selectable.
     */
    public static final String DEFAULT_FTS_TOKENIZER = "simple";

    private LanceIndexBuilder() {}

    /** A column the builder left alone, with the reason. */
    public record Skipped(String column, String reason) {
    }

    /**
     * A column whose build threw. {@link #reason()} carries Lance's
     * message. {@link #invalidInput()} is true when Lance's JNI mapped the
     * error to {@link IllegalArgumentException} (unknown tokenizer,
     * malformed index params), which the caller can answer as a client
     * error rather than a server one.
     */
    public record Failed(String column, String reason, boolean invalidInput) {
        public static Failed of(String column, Exception cause) {
            return new Failed(column, messageOf(cause), isInvalidInput(cause));
        }
    }

    private static boolean isInvalidInput(Exception cause) {
        return cause instanceof IllegalArgumentException;
    }

    private static String messageOf(Exception e) {
        String message = e.getMessage();
        return message == null || message.isEmpty() ? e.toString() : message;
    }

    /**
     * Per-column outcome of one build pass. Mutable while the builder runs,
     * read-only for callers through the accessors.
     */
    public static final class BuildResult {
        private final List<String> built = new ArrayList<>();
        private final List<Skipped> skipped = new ArrayList<>();
        private final List<Failed> failed = new ArrayList<>();

        /** Columns (or, for the optimize path, index names) that received a commit. */
        public List<String> built() {
            return Collections.unmodifiableList(built);
        }

        public List<Skipped> skipped() {
            return Collections.unmodifiableList(skipped);
        }

        public List<Failed> failed() {
            return Collections.unmodifiableList(failed);
        }

        private void addBuilt(String name) {
            built.add(name);
        }

        private void addSkipped(String column, String reason) {
            skipped.add(new Skipped(column, reason));
        }

        private void addFailed(String column, Exception cause) {
            failed.add(Failed.of(column, cause));
        }

        private void addFailed(Failed failure) {
            failed.add(failure);
        }
    }

    /**
     * Builds FTS indexes for the given target columns. Columns already carrying
     * an FTS index are skipped unless {@code fragmentIds} is present, in which
     * case the specified fragments are added to the existing index.
     *
     * <p>{@code tokenizer} is handed to Lance verbatim as the inverted
     * index's {@code base_tokenizer} ({@code simple}, {@code whitespace},
     * {@code raw}, {@code icu}, {@code lindera/ipadic}, {@code jieba/default}
     * and whatever else the loaded Lance native library accepts). The plugin
     * keeps no list of valid names because the set changes with the Lance
     * version; when Lance rejects the value (unknown name, or a dictionary
     * directory that is missing under {@code LANCE_LANGUAGE_MODEL_HOME}) the
     * JNI layer raises {@link IllegalArgumentException}, which lands in
     * {@link BuildResult#failed()} with {@link Failed#invalidInput()} set.
     */
    public static BuildResult ensureFtsIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds,
        String tokenizer
    ) {
        BuildResult result = new BuildResult();
        if (targetColumns.isEmpty()) {
            return result;
        }
        if (rowsUnlessOverMaxRows(dataset, maxRows, "FTS", targetColumns, result).isEmpty()) {
            return result;
        }
        String ftsParamsJson = ftsParamsJson(tokenizer);
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
                if (hasFts) {
                    result.addSkipped(column, existingIndexReason("FTS", fragmentIds));
                    continue;
                }
                ScalarIndexParams fts = ScalarIndexParams.create("inverted", ftsParamsJson);
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
                result.addBuilt(column);
                LOG.info(
                    "built FTS index over column {} (tokenizer={}, fragmentIds={}, version {})",
                    column,
                    tokenizer,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("FTS build failed for column {}", column, e);
                result.addFailed(column, e);
            }
        }
        return result;
    }

    /**
     * Serialises the inverted index params Lance reads at CreateIndex.
     * Goes through an {@link XContentBuilder} so a tokenizer name with a
     * quote or backslash cannot break out of the JSON string.
     */
    private static String ftsParamsJson(String tokenizer) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("base_tokenizer", tokenizer).endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Builds BTree scalar indexes for numeric / date / boolean / keyword columns.
     * Columns already carrying a scalar index are skipped.
     */
    public static BuildResult ensureScalarIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds
    ) {
        BuildResult result = new BuildResult();
        if (targetColumns.isEmpty()) {
            return result;
        }
        if (rowsUnlessOverMaxRows(dataset, maxRows, "scalar", targetColumns, result).isEmpty()) {
            return result;
        }
        for (Field field : dataset.getSchema().getFields()) {
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            try {
                if (hasScalarIndex(dataset, column)) {
                    result.addSkipped(column, existingIndexReason("scalar", fragmentIds));
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
                result.addBuilt(column);
                LOG.info(
                    "built BTree scalar index over column {} (fragmentIds={}, version {})",
                    column,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("scalar build failed for column {}", column, e);
                result.addFailed(column, e);
            }
        }
        return result;
    }

    /**
     * Builds IVF_PQ vector indexes for FixedSizeList&lt;float&gt; columns.
     * Skips when {@code dataset.countRows()} is below the IVF_PQ training
     * minimum (256). Columns already carrying a vector index are skipped;
     * extending one over new fragments goes through the optimize path.
     */
    public static BuildResult ensureVectorIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds
    ) {
        BuildResult result = new BuildResult();
        if (targetColumns.isEmpty()) {
            return result;
        }
        OptionalLong counted = rowsUnlessOverMaxRows(dataset, maxRows, "vector", targetColumns, result);
        if (counted.isEmpty()) {
            return result;
        }
        long rows = counted.getAsLong();
        if (rows < IVF_PQ_MIN_ROWS) {
            LOG.warn("skipping vector build over {}: table has {} rows, below IVF_PQ minimum {}", targetColumns, rows, IVF_PQ_MIN_ROWS);
            for (String column : targetColumns) {
                result.addSkipped(column, "table has " + rows + " rows, below the IVF_PQ training minimum of " + IVF_PQ_MIN_ROWS);
            }
            return result;
        }
        for (Field field : dataset.getSchema().getFields()) {
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            try {
                if (hasVectorIndex(dataset, column)) {
                    result.addSkipped(column, existingIndexReason("vector", fragmentIds));
                    continue;
                }
                // Initial build only in this method. train=true because we only
                // reach this branch when the index does not yet exist. Adding
                // fragments to an existing vector index goes through the
                // optimize path.
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
                result.addBuilt(column);
                LOG.info(
                    "built IVF_PQ vector index over column {} (fragmentIds={}, version {})",
                    column,
                    fragmentIds.orElse(null),
                    dataset.version()
                );
            } catch (Exception e) {
                LOG.warn("vector build failed for column {}", column, e);
                result.addFailed(column, e);
            }
        }
        return result;
    }

    /**
     * Reason string for a column whose index already exists. A partial
     * build ({@code fragment_ids}) is not applied to an existing index
     * either, so both shapes point the caller at {@code optimize=true}.
     */
    private static String existingIndexReason(String kind, Optional<List<Integer>> fragmentIds) {
        if (fragmentIds.isPresent()) {
            return kind + " index already exists; fragment_ids does not extend an existing index, use optimize=true";
        }
        return kind + " index already exists; use optimize=true to extend it over new fragments";
    }

    /**
     * Applies the {@code maxRows} load control. Returns the row count when
     * the caller may go on, or empty when it must stop, having recorded
     * every target column as skipped (table over the cap) or failed
     * ({@code countRows} threw).
     */
    private static OptionalLong rowsUnlessOverMaxRows(
        Dataset dataset,
        long maxRows,
        String kind,
        Set<String> targetColumns,
        BuildResult result
    ) {
        long rows;
        try {
            rows = dataset.countRows();
        } catch (Exception e) {
            LOG.warn("countRows failed for {} build over {}", kind, targetColumns, e);
            for (String column : targetColumns) {
                result.addFailed(column, e);
            }
            return OptionalLong.empty();
        }
        if (rows > maxRows) {
            LOG.warn("skipping {} build over {}: table has {} rows, exceeds lance.builder.max_rows={}", kind, targetColumns, rows, maxRows);
            for (String column : targetColumns) {
                result.addSkipped(column, "table has " + rows + " rows, exceeds lance.builder.max_rows=" + maxRows);
            }
            return OptionalLong.empty();
        }
        return OptionalLong.of(rows);
    }

    /**
     * Extends the FTS index of every column in {@code columns} that already
     * carries one (through {@link Dataset#optimizeIndices}), so an appended
     * fragment is picked up before the reader sees the new manifest. Columns
     * without an FTS index are reported as skipped; a matching
     * {@code ensureFtsIndexes} call is expected to create them separately.
     * {@link BuildResult#built()} lists the Lance index names that were
     * optimized, not the columns.
     */
    public static BuildResult optimizeExistingFtsIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        return optimizeExisting(dataset, columns, retrain, "FTS", desc -> true, true);
    }

    /**
     * Same shape as {@link #optimizeExistingFtsIndexes(Dataset, Set, boolean)}
     * but limited to scalar index types (BTree / Bitmap / LabelList / ZoneMap /
     * NGram / BloomFilter). Vector and FTS indexes on the same column stay
     * untouched.
     */
    public static BuildResult optimizeExistingScalarIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        return optimizeExisting(dataset, columns, retrain, "scalar", desc -> isScalarIndexType(desc.getIndexType()), false);
    }

    /**
     * Same shape as {@link #optimizeExistingFtsIndexes(Dataset, Set, boolean)}
     * but limited to vector index types (IVF_*, Vector). Keeps the wait
     * policy honest for KNN queries after an append.
     */
    public static BuildResult optimizeExistingVectorIndexes(Dataset dataset, Set<String> columns, boolean retrain) {
        return optimizeExisting(dataset, columns, retrain, "vector", desc -> isVectorIndexType(desc.getIndexType()), false);
    }

    /**
     * Shared optimize path. Collects the index names each column carries
     * (filtered by {@code selector}; {@code ftsOnly} uses Lance's own
     * {@code mustSupportFts} criterion instead), then runs one
     * {@link Dataset#optimizeIndices} over all of them. Lance's
     * {@link OptimizeOptions} does not accept fragment ids; the merge covers
     * every out-of-index fragment automatically. When that single call
     * throws, every column that contributed an index name is reported as
     * failed with the same cause, because Lance does not say which index
     * broke the batch.
     */
    private static BuildResult optimizeExisting(
        Dataset dataset,
        Set<String> columns,
        boolean retrain,
        String kind,
        Predicate<IndexDescription> selector,
        boolean ftsOnly
    ) {
        BuildResult result = new BuildResult();
        if (columns.isEmpty()) {
            return result;
        }
        Map<String, List<String>> indexNamesByColumn = new LinkedHashMap<>();
        for (String column : columns) {
            try {
                IndexCriteria.Builder criteria = new IndexCriteria.Builder().forColumn(column);
                if (ftsOnly) {
                    criteria.mustSupportFts(true);
                }
                List<String> names = new ArrayList<>();
                for (IndexDescription desc : dataset.describeIndices(criteria.build())) {
                    if (selector.test(desc)) {
                        names.add(desc.getName());
                    }
                }
                if (names.isEmpty()) {
                    result.addSkipped(column, "no " + kind + " index to optimize; build one first");
                } else {
                    indexNamesByColumn.put(column, names);
                }
            } catch (Exception e) {
                LOG.warn("describeIndices failed for column {}", column, e);
                result.addFailed(column, e);
            }
        }
        if (indexNamesByColumn.isEmpty()) {
            return result;
        }
        List<String> indexNames = new ArrayList<>();
        indexNamesByColumn.values().forEach(indexNames::addAll);
        try {
            dataset.optimizeIndices(OptimizeOptions.builder().indexNames(indexNames).retrain(retrain).build());
            LOG.info("optimized {} indexes {} (retrain={}, version {})", kind, indexNames, retrain, dataset.version());
            indexNames.forEach(result::addBuilt);
        } catch (Exception e) {
            LOG.warn("optimize failed for {} indexes {}", kind, indexNames, e);
            // Same cause for every column in the batch, with the column's
            // index names in the reason and Lance's classification kept,
            // so an invalid-input rejection still answers as one.
            for (Map.Entry<String, List<String>> entry : indexNamesByColumn.entrySet()) {
                result.addFailed(
                    new Failed(entry.getKey(), "optimize of " + entry.getValue() + " failed: " + messageOf(e), isInvalidInput(e))
                );
            }
        }
        return result;
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
        return type == null ? "" : type.replace("_", "").toUpperCase(Locale.ROOT);
    }

    private static boolean isScalarIndexType(String type) {
        return SCALAR_INDEX_TYPES.contains(normaliseIndexType(type));
    }

    // Any Lance vector index type (IVF_Flat, IVF_PQ, IVF_SQ, IVF_HNSW_SQ,
    // IVF_HNSW_PQ, IVF_HNSW_FLAT, IVF_RQ, Vector).
    private static boolean isVectorIndexType(String type) {
        String normalised = normaliseIndexType(type);
        return normalised.startsWith("IVF") || "VECTOR".equals(normalised);
    }

    private static boolean hasScalarIndex(Dataset dataset, String column) {
        for (IndexDescription desc : dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build())) {
            if (isScalarIndexType(desc.getIndexType())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasVectorIndex(Dataset dataset, String column) {
        for (IndexDescription desc : dataset.describeIndices(new IndexCriteria.Builder().forColumn(column).build())) {
            if (isVectorIndexType(desc.getIndexType())) {
                return true;
            }
        }
        return false;
    }
}
