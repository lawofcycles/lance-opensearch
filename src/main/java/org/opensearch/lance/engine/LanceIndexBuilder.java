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
import org.lance.index.vector.HnswBuildParams;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.PQBuildParams;
import org.lance.index.vector.RQBuildParams;
import org.lance.index.vector.SQBuildParams;
import org.lance.index.vector.VectorIndexParams;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.LanceOverrides;
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
 * Vector index types have per-type training minimums (see
 * {@link #vectorTrainingMinimum}); smaller tables skip the vector build.
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
     * A Lance index the builder committed: the column it covers (or,
     * for the optimize path, the Lance index name) and the index type
     * that was built. On the create path {@link #type()} is the
     * {@link IndexType} name that was requested ({@code BTREE},
     * {@code BITMAP}, {@code IVF_FLAT}, ...); on the optimize path it
     * is the type string Lance's {@code describeIndices} reports for
     * the merged index.
     */
    public record Built(String column, String type) {
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
        private final List<Built> built = new ArrayList<>();
        private final List<Skipped> skipped = new ArrayList<>();
        private final List<Failed> failed = new ArrayList<>();

        /**
         * Indexes that received a commit: the column (or, for the
         * optimize path, the index name) and the index type.
         */
        public List<Built> built() {
            return Collections.unmodifiableList(built);
        }

        public List<Skipped> skipped() {
            return Collections.unmodifiableList(skipped);
        }

        public List<Failed> failed() {
            return Collections.unmodifiableList(failed);
        }

        private void addBuilt(String name, String type) {
            built.add(new Built(name, type));
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
     *
     * <p>{@code withPosition} asks Lance to store token positions in the
     * inverted index (Lance's {@code with_position}). Phrase queries
     * ({@code lance_match_phrase}) need them; an index built without
     * positions rejects phrase queries at query time. Lance's default is
     * false, and so is the plugin's.
     */
    public static BuildResult ensureFtsIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds,
        String tokenizer,
        boolean withPosition
    ) {
        BuildResult result = new BuildResult();
        if (targetColumns.isEmpty()) {
            return result;
        }
        if (rowsUnlessOverMaxRows(dataset, maxRows, "FTS", targetColumns, result).isEmpty()) {
            return result;
        }
        String ftsParamsJson = ftsParamsJson(tokenizer, withPosition);
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
                result.addBuilt(column, IndexType.INVERTED.name());
                LOG.info(
                    "built FTS index over column {} (tokenizer={}, with_position={}, fragmentIds={}, version {})",
                    column,
                    tokenizer,
                    withPosition,
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
     * {@code with_position} is written even when false so the log and the
     * params Lance stores say what was asked for.
     */
    private static String ftsParamsJson(String tokenizer, boolean withPosition) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("base_tokenizer", tokenizer).field("with_position", withPosition).endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Builds scalar indexes for numeric / date / boolean / keyword columns.
     * Columns already carrying a scalar index are skipped. The index type
     * per column comes from {@code preferences} (the {@code indexes}
     * clause of the attach or namespace body, or the one-shot
     * {@code build_indexes} body): a preference of {@code none} skips the
     * column, an absent preference builds the default BTree. The
     * preference's {@code params} serialise into the type's Lance option
     * JSON.
     */
    public static BuildResult ensureScalarIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds,
        Map<String, LanceOverrides.IndexPreference> preferences
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
            LanceOverrides.IndexPreference preference = preferences.get(column);
            String typeName = preference != null && preference.scalar() != null ? preference.scalar() : "btree";
            if (LanceOverrides.INDEX_NONE.equals(typeName)) {
                result.addSkipped(column, "index type none");
                continue;
            }
            IndexType indexType = scalarIndexType(typeName);
            try {
                if (hasScalarIndex(dataset, column)) {
                    result.addSkipped(column, existingIndexReason("scalar", fragmentIds));
                    continue;
                }
                String paramsJson = scalarParamsJson(preference);
                ScalarIndexParams scalarParams = paramsJson == null
                    ? ScalarIndexParams.create(typeName)
                    : ScalarIndexParams.create(typeName, paramsJson);
                String indexName = column + "_" + typeName;
                IndexOptions.Builder builder = IndexOptions.builder(
                    Collections.singletonList(column),
                    indexType,
                    IndexParams.builder().setScalarIndexParams(scalarParams).build()
                ).withIndexName(indexName);
                fragmentIds.ifPresent(builder::withFragmentIds);
                Index segment = dataset.createIndex(builder.build());
                if (fragmentIds.isPresent()) {
                    dataset.commitExistingIndexSegments(indexName, column, Collections.singletonList(segment));
                }
                result.addBuilt(column, indexType.name());
                LOG.info(
                    "built {} scalar index over column {} (params={}, fragmentIds={}, version {})",
                    indexType,
                    column,
                    paramsJson,
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
     * The Lance {@link IndexType} behind one accepted scalar type name
     * of the {@code indexes} clause.
     */
    private static IndexType scalarIndexType(String typeName) {
        switch (typeName) {
            case "btree":
                return IndexType.BTREE;
            case "bitmap":
                return IndexType.BITMAP;
            case "zonemap":
                return IndexType.ZONEMAP;
            case "bloomfilter":
                return IndexType.BLOOM_FILTER;
            case "ngram":
                return IndexType.NGRAM;
            case "labellist":
                return IndexType.LABEL_LIST;
            default:
                throw new IllegalArgumentException("unknown scalar index type [" + typeName + "]");
        }
    }

    /**
     * Serialises the preference's {@code params} into the JSON Lance's
     * scalar index builder reads ({@code zone_size},
     * {@code rows_per_zone}, {@code number_of_items},
     * {@code probability}). {@code null} when the preference carries no
     * params, so the type's Lance defaults apply. Goes through an
     * {@link XContentBuilder} for correct JSON escaping and number
     * shapes.
     */
    private static String scalarParamsJson(LanceOverrides.IndexPreference preference) {
        if (preference == null || preference.params().isEmpty()) {
            return null;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Number> param : preference.params().entrySet()) {
                builder.field(param.getKey(), param.getValue());
            }
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Builds vector indexes for FixedSizeList&lt;float&gt; columns. The
     * index type per column comes from {@code preferences}; without one
     * the default IVF_PQ (one partition, 8 sub-vectors, 8 bits, L2) is
     * built, exactly as before the type became selectable. Columns whose
     * table holds fewer rows than the chosen type's training minimum are
     * skipped: a product-quantised type (IVF_PQ, IVF_HNSW_PQ) needs
     * {@code 2^num_bits} rows to train its codebook (Lance refuses with
     * "Not enough rows to train PQ", {@code rust/lance/src/index/vector/pq.rs}),
     * and every IVF type needs at least {@code num_partitions} rows for
     * k-means to form its partitions
     * ({@code rust/lance-index/src/vector/kmeans.rs}). Columns already
     * carrying a vector index are skipped; extending one over new
     * fragments goes through the optimize path.
     */
    public static BuildResult ensureVectorIndexes(
        Dataset dataset,
        Set<String> targetColumns,
        long maxRows,
        Optional<List<Integer>> fragmentIds,
        Map<String, LanceOverrides.IndexPreference> preferences
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
        for (Field field : dataset.getSchema().getFields()) {
            String column = field.getName();
            if (!targetColumns.contains(column)) {
                continue;
            }
            LanceOverrides.IndexPreference preference = preferences.get(column);
            String typeName = preference != null && preference.vector() != null ? preference.vector() : "ivf_pq";
            if (LanceOverrides.INDEX_NONE.equals(typeName)) {
                result.addSkipped(column, "index type none");
                continue;
            }
            Map<String, Number> params = preference == null ? Map.of() : preference.params();
            long minRows = vectorTrainingMinimum(typeName, params);
            if (rows < minRows) {
                LOG.warn("skipping {} build over {}: table has {} rows, below the training minimum {}", typeName, column, rows, minRows);
                result.addSkipped(column, "table has " + rows + " rows, below the " + typeName + " training minimum of " + minRows);
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
                VectorIndexParams vectorParams = vectorIndexParams(typeName, params);
                IndexType indexType = vectorIndexType(typeName);
                String indexName = column + "_vec";
                IndexOptions.Builder builder = IndexOptions.builder(
                    Collections.singletonList(column),
                    indexType,
                    IndexParams.builder().setVectorIndexParams(vectorParams).build()
                ).withIndexName(indexName).train(true);
                fragmentIds.ifPresent(builder::withFragmentIds);
                Index segment = dataset.createIndex(builder.build());
                if (fragmentIds.isPresent()) {
                    dataset.commitExistingIndexSegments(indexName, column, Collections.singletonList(segment));
                }
                result.addBuilt(column, indexType.name());
                LOG.info(
                    "built {} vector index over column {} (fragmentIds={}, version {})",
                    indexType,
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

    private static IndexType vectorIndexType(String typeName) {
        switch (typeName) {
            case "ivf_flat":
                return IndexType.IVF_FLAT;
            case "ivf_pq":
                return IndexType.IVF_PQ;
            case "ivf_sq":
                return IndexType.IVF_SQ;
            case "ivf_rq":
                return IndexType.IVF_RQ;
            case "ivf_hnsw_pq":
                return IndexType.IVF_HNSW_PQ;
            case "ivf_hnsw_sq":
                return IndexType.IVF_HNSW_SQ;
            default:
                throw new IllegalArgumentException("unknown vector index type [" + typeName + "]");
        }
    }

    /**
     * The rows a table must hold before {@code typeName} can train.
     * Product-quantised types need {@code 2^num_bits} training vectors
     * for the codebook (default {@code num_bits} 8, hence the familiar
     * 256); every IVF type needs at least {@code num_partitions} rows
     * for k-means to produce its partitions. The plugin's default
     * {@code num_partitions} is 1, so a bare {@code ivf_flat} has no
     * effective row minimum.
     */
    static long vectorTrainingMinimum(String typeName, Map<String, Number> params) {
        long numPartitions = longParam(params, "num_partitions", 1L);
        long minimum = Math.max(1L, numPartitions);
        if ("ivf_pq".equals(typeName) || "ivf_hnsw_pq".equals(typeName)) {
            long numBits = longParam(params, "num_bits", 8L);
            minimum = Math.max(minimum, 1L << numBits);
        }
        return minimum;
    }

    private static long longParam(Map<String, Number> params, String key, long defaultValue) {
        Number value = params.get(key);
        return value == null ? defaultValue : value.longValue();
    }

    private static int intParam(Map<String, Number> params, String key, int defaultValue) {
        Number value = params.get(key);
        return value == null ? defaultValue : value.intValue();
    }

    /**
     * Builds the {@link VectorIndexParams} for one accepted vector type
     * name and its {@code params}. Distance type is always L2 (the
     * plugin's default). {@code num_partitions} defaults to 1 for every
     * type, matching the default IVF_PQ build the plugin has always
     * performed; the quantiser parameters default to the Lance Java
     * SDK's own defaults except IVF_PQ's {@code num_sub_vectors} (8) and
     * its PQ training iterations (20), which stay at the values of the
     * previous fixed build.
     */
    static VectorIndexParams vectorIndexParams(String typeName, Map<String, Number> params) {
        int numPartitions = intParam(params, "num_partitions", 1);
        IvfBuildParams.Builder ivf = new IvfBuildParams.Builder().setNumPartitions(numPartitions);
        Number sampleRate = params.get("sample_rate");
        if (sampleRate != null) {
            ivf.setSampleRate(sampleRate.intValue());
        }
        switch (typeName) {
            case "ivf_flat":
                return new VectorIndexParams.Builder(ivf.build()).setDistanceType(DistanceType.L2).build();
            case "ivf_pq": {
                PQBuildParams pq = new PQBuildParams.Builder().setNumSubVectors(intParam(params, "num_sub_vectors", 8))
                    .setNumBits(intParam(params, "num_bits", 8))
                    .setMaxIters(20)
                    .build();
                return VectorIndexParams.withIvfPqParams(DistanceType.L2, ivf.build(), pq);
            }
            case "ivf_sq": {
                SQBuildParams.Builder sq = new SQBuildParams.Builder();
                Number numBits = params.get("num_bits");
                if (numBits != null) {
                    sq.setNumBits(numBits.shortValue());
                }
                return new VectorIndexParams.Builder(ivf.build()).setDistanceType(DistanceType.L2).setSqParams(sq.build()).build();
            }
            case "ivf_rq": {
                RQBuildParams.Builder rq = new RQBuildParams.Builder();
                Number numBits = params.get("num_bits");
                if (numBits != null) {
                    rq.setNumBits(numBits.byteValue());
                }
                return new VectorIndexParams.Builder(ivf.build()).setDistanceType(DistanceType.L2).setRqParams(rq.build()).build();
            }
            case "ivf_hnsw_pq": {
                HnswBuildParams hnsw = hnswParams(params);
                PQBuildParams pq = new PQBuildParams.Builder().setNumSubVectors(intParam(params, "num_sub_vectors", 8))
                    .setNumBits(intParam(params, "num_bits", 8))
                    .build();
                return VectorIndexParams.withIvfHnswPqParams(DistanceType.L2, ivf.build(), hnsw, pq);
            }
            case "ivf_hnsw_sq": {
                HnswBuildParams hnsw = hnswParams(params);
                SQBuildParams.Builder sq = new SQBuildParams.Builder();
                Number numBits = params.get("num_bits");
                if (numBits != null) {
                    sq.setNumBits(numBits.shortValue());
                }
                return VectorIndexParams.withIvfHnswSqParams(DistanceType.L2, ivf.build(), hnsw, sq.build());
            }
            default:
                throw new IllegalArgumentException("unknown vector index type [" + typeName + "]");
        }
    }

    private static HnswBuildParams hnswParams(Map<String, Number> params) {
        HnswBuildParams.Builder hnsw = new HnswBuildParams.Builder();
        Number m = params.get("m");
        if (m != null) {
            hnsw.setM(m.intValue());
        }
        Number efConstruction = params.get("ef_construction");
        if (efConstruction != null) {
            hnsw.setEfConstruction(efConstruction.intValue());
        }
        return hnsw.build();
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
        Map<String, List<Built>> indexesByColumn = new LinkedHashMap<>();
        for (String column : columns) {
            try {
                IndexCriteria.Builder criteria = new IndexCriteria.Builder().forColumn(column);
                if (ftsOnly) {
                    criteria.mustSupportFts(true);
                }
                List<Built> names = new ArrayList<>();
                for (IndexDescription desc : dataset.describeIndices(criteria.build())) {
                    if (selector.test(desc)) {
                        names.add(new Built(desc.getName(), desc.getIndexType()));
                    }
                }
                if (names.isEmpty()) {
                    result.addSkipped(column, "no " + kind + " index to optimize; build one first");
                } else {
                    indexesByColumn.put(column, names);
                }
            } catch (Exception e) {
                LOG.warn("describeIndices failed for column {}", column, e);
                result.addFailed(column, e);
            }
        }
        if (indexesByColumn.isEmpty()) {
            return result;
        }
        List<String> indexNames = new ArrayList<>();
        for (List<Built> entries : indexesByColumn.values()) {
            for (Built entry : entries) {
                indexNames.add(entry.column());
            }
        }
        try {
            dataset.optimizeIndices(OptimizeOptions.builder().indexNames(indexNames).retrain(retrain).build());
            LOG.info("optimized {} indexes {} (retrain={}, version {})", kind, indexNames, retrain, dataset.version());
            for (List<Built> entries : indexesByColumn.values()) {
                for (Built entry : entries) {
                    result.addBuilt(entry.column(), entry.type());
                }
            }
        } catch (Exception e) {
            LOG.warn("optimize failed for {} indexes {}", kind, indexNames, e);
            // Same cause for every column in the batch, with the column's
            // index names in the reason and Lance's classification kept,
            // so an invalid-input rejection still answers as one.
            for (Map.Entry<String, List<Built>> entry : indexesByColumn.entrySet()) {
                List<String> names = new ArrayList<>();
                for (Built built : entry.getValue()) {
                    names.add(built.column());
                }
                result.addFailed(new Failed(entry.getKey(), "optimize of " + names + " failed: " + messageOf(e), isInvalidInput(e)));
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
