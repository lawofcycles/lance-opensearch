/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;

/**
 * What a {@link LanceFragmentLeafReader} knows about its table before it
 * reads any row: which Arrow columns are surfaced and as what kind, the
 * primary key, the multi-field sub-fields, the projection of the per-hit
 * row take and the Lucene {@link FieldInfos}. All of it is a function of
 * the Lance schema at one manifest version plus the index settings
 * (primary key, multi-fields), so it is derived once and shared by every
 * leaf over that version: the leaves of one request, and, through
 * {@link LanceWarmCache}, the leaves of every request against the same
 * snapshot.
 *
 * <p>Immutable after {@link #derive}.
 */
public final class LanceFragmentSchema {

    /**
     * Column kind resolved from the fragment schema plus the set of Utf8
     * columns that carry an FTS index (see {@link #resolveFtsColumns}).
     */
    enum ColumnKind {
        NUMERIC,       // signed Int / Date / Timestamp / Float32 / Float64 — served as NumericDocValues
        BOOLEAN,       // Bool — served as NumericDocValues
        TEXT_FTS,      // Utf8 with a Lance FTS index — FieldInfo only, no doc values
        TEXT_KEYWORD,  // Utf8 without an FTS index — SortedSetDocValues via ords
        KEYWORD_ARRAY, // List<Utf8> — multi-valued SortedSetDocValues
        BINARY         // Binary / LargeBinary — FieldInfo only, values fetched for _source
    }

    /**
     * Encoding of a numeric value once it has been read as a {@code long}.
     * Every numeric column shares the same {@code long} representation so
     * that {@link org.apache.lucene.index.NumericDocValues} can hand back
     * a {@code long} per doc regardless of the underlying Arrow type. The
     * enum records how to render that {@code long} back to a JSON value
     * inside {@code _source} and how downstream OpenSearch field mappers
     * decode the value on read.
     *
     * <ul>
     *   <li>{@link #INTEGER} — a plain integer (default). Signed integer
     *       columns and date / timestamp columns fall in here; the
     *       epoch-millis normalisation done inside
     *       {@link LanceFragmentLeafReader#readAsLong} means the
     *       {@code DateFieldMapper} decodes them directly.</li>
     *   <li>{@link #FLOAT} — a {@code float32} column whose values are
     *       stored as {@code NumericUtils.floatToSortableInt} results
     *       widened to {@code long}. OpenSearch's {@code float} field
     *       type decodes the stored value via
     *       {@code NumericUtils.sortableIntToFloat((int) longValue)}.</li>
     *   <li>{@link #DOUBLE} — a {@code float64} column, mirrored to
     *       {@code NumericUtils.doubleToSortableLong} on the write side
     *       and {@code sortableLongToDouble} on the read side.</li>
     * </ul>
     */
    enum NumericPrecision {
        INTEGER,
        FLOAT,
        DOUBLE
    }

    private final String fieldName;
    private final LancePrimaryKeyType pkType;
    private final LinkedHashMap<String, ColumnKind> columnKind;
    private final Map<String, NumericPrecision> numericPrecision;
    private final Map<String, String> keywordSubFields;
    private final Set<String> basesWithKeywordSub;
    private final List<String> takeColumns;
    private final int sourceColumnCount;
    private final int pkTakeIndex;
    private final FieldInfos fieldInfos;

    private LanceFragmentSchema(
        String fieldName,
        LancePrimaryKeyType pkType,
        LinkedHashMap<String, ColumnKind> columnKind,
        Map<String, NumericPrecision> numericPrecision,
        Map<String, String> keywordSubFields,
        Set<String> basesWithKeywordSub,
        List<String> takeColumns,
        int sourceColumnCount,
        int pkTakeIndex,
        FieldInfos fieldInfos
    ) {
        this.fieldName = fieldName;
        this.pkType = pkType;
        this.columnKind = columnKind;
        this.numericPrecision = numericPrecision;
        this.keywordSubFields = keywordSubFields;
        this.basesWithKeywordSub = basesWithKeywordSub;
        this.takeColumns = takeColumns;
        this.sourceColumnCount = sourceColumnCount;
        this.pkTakeIndex = pkTakeIndex;
        this.fieldInfos = fieldInfos;
    }

    /**
     * Resolve which Utf8 columns of {@code dataset} carry an FTS
     * (inverted) index. One {@code describeIndices} round trip per Utf8
     * column, so callers run this once per reader open (or once per
     * cached snapshot) and share the result across leaves.
     *
     * @return set of column names that have an index supporting FTS;
     *         never {@code null}, possibly empty
     */
    public static Set<String> resolveFtsColumns(Dataset dataset) throws IOException {
        Set<String> fts = new HashSet<>();
        try {
            for (Field field : dataset.getSchema().getFields()) {
                if (!(field.getType() instanceof ArrowType.Utf8)) {
                    continue;
                }
                boolean hasFts = !dataset.describeIndices(
                    new IndexCriteria.Builder().forColumn(field.getName()).mustSupportFts(true).build()
                ).isEmpty();
                if (hasFts) {
                    fts.add(field.getName());
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return fts;
    }

    /**
     * Derive the schema of every leaf over {@code dataset}. Metadata only:
     * reads {@code dataset.getSchema()} and nothing else.
     *
     * @param dataset     the Lance dataset at the version the leaves read
     * @param intField    primary key column name, or empty when the table
     *                    declares no primary key
     * @param pkType      Arrow type family of the primary key; overridden
     *                    to {@link LancePrimaryKeyType#NONE} when
     *                    {@code intField} is empty
     * @param multiFields attach-body multi-fields spec, nullable
     * @param ftsColumns  Utf8 columns with an FTS index, from
     *                    {@link #resolveFtsColumns}
     */
    public static LanceFragmentSchema derive(
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        Map<String, LinkedHashMap<String, String>> multiFields,
        Set<String> ftsColumns
    ) throws IOException {
        // Empty field name overrides pkType regardless of what the caller
        // passed in, so every accessor agrees on "no PK" without the
        // caller having to zip the two settings.
        LancePrimaryKeyType effectivePkType = intField.isEmpty() ? LancePrimaryKeyType.NONE : pkType;

        // Flatten the multi-fields spec into "<sub>" → "<base>" lookup so
        // getSortedDocValues("body.raw") can route to the base column's
        // ord data structure without re-parsing the spec. Only keyword
        // sub-fields are supported; any other sub-field type is skipped
        // here because attach-time validation is where the error surfaces.
        LinkedHashMap<String, String> subToBase = new LinkedHashMap<>();
        Set<String> basesWithKeywordSub = new HashSet<>();
        if (multiFields != null && !multiFields.isEmpty()) {
            for (Map.Entry<String, LinkedHashMap<String, String>> entry : multiFields.entrySet()) {
                String baseName = entry.getKey();
                for (Map.Entry<String, String> sub : entry.getValue().entrySet()) {
                    if (!"keyword".equals(sub.getValue())) {
                        continue;
                    }
                    subToBase.put(baseName + "." + sub.getKey(), baseName);
                    basesWithKeywordSub.add(baseName);
                }
            }
        }

        // Schema pass: classify every column we might surface. FTS
        // presence for Utf8 columns comes from the caller-resolved
        // ftsColumns set rather than a per-leaf JNI call.
        LinkedHashMap<String, ColumnKind> columnKind = new LinkedHashMap<>();
        Map<String, NumericPrecision> numericPrecision = new LinkedHashMap<>();
        try {
            for (Field field : dataset.getSchema().getFields()) {
                ColumnKind kind = classify(field);
                if (kind == null) {
                    continue;
                }
                if (kind == ColumnKind.TEXT_FTS) {
                    kind = ftsColumns.contains(field.getName()) ? ColumnKind.TEXT_FTS : ColumnKind.TEXT_KEYWORD;
                }
                columnKind.put(field.getName(), kind);
                // Remember the precision of Float32 / Float64 columns so
                // _source and readAsLong can round trip them through the
                // shared long representation. All other numeric columns
                // default to INTEGER via getOrDefault.
                if (field.getType() instanceof ArrowType.FloatingPoint fp) {
                    if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                        numericPrecision.put(field.getName(), NumericPrecision.FLOAT);
                    } else if (fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
                        numericPrecision.put(field.getName(), NumericPrecision.DOUBLE);
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        // An UNSIGNED_LONG primary key sits on a Lance UInt64 column that
        // classify() otherwise refuses (unsigned integers are not
        // surfaced by default). Force it into NUMERIC so the PK gets a
        // NumericDocValues entry; readAsLong already returns the unsigned
        // bit pattern for UInt8Vector. Only this one column is elevated.
        if (effectivePkType == LancePrimaryKeyType.UNSIGNED_LONG && !intField.isEmpty()) {
            columnKind.putIfAbsent(intField, ColumnKind.NUMERIC);
        }

        // Projection for the per-hit row take: every surfaced column in
        // schema order so _source keys come out in a stable order, plus
        // the PK column appended when its Arrow type is one classify()
        // declines (the take still needs it for _id).
        List<String> take = new ArrayList<>(columnKind.keySet());
        int sourceColumnCount = take.size();
        int pkIndex = -1;
        if (effectivePkType != LancePrimaryKeyType.NONE) {
            pkIndex = take.indexOf(intField);
            if (pkIndex < 0) {
                take.add(intField);
                pkIndex = take.size() - 1;
            }
        }

        // FieldInfos derived from columnKind. Field numbers start at 10 to
        // leave 1 / 2 free for _id and _source. One entry per column:
        // NUMERIC / BOOLEAN → NumericDocValues, TEXT_KEYWORD /
        // KEYWORD_ARRAY → SortedSet, TEXT_FTS / BINARY → no doc values but
        // the FieldInfo exists so the security plugin's FLS wrapper can
        // drop them by name. Multi-fields append a synthetic SORTED_SET
        // entry per keyword sub-field; the data lives on the base column.
        List<FieldInfo> infos = new ArrayList<>();
        int number = 10;
        for (Map.Entry<String, ColumnKind> entry : columnKind.entrySet()) {
            DocValuesType dvType = switch (entry.getValue()) {
                case NUMERIC, BOOLEAN -> DocValuesType.NUMERIC;
                case TEXT_KEYWORD, KEYWORD_ARRAY -> DocValuesType.SORTED_SET;
                case TEXT_FTS, BINARY -> DocValuesType.NONE;
            };
            infos.add(fieldInfo(entry.getKey(), number++, dvType));
        }
        for (String subName : subToBase.keySet()) {
            infos.add(fieldInfo(subName, number++, DocValuesType.SORTED_SET));
        }

        return new LanceFragmentSchema(
            intField,
            effectivePkType,
            columnKind,
            Collections.unmodifiableMap(numericPrecision),
            Collections.unmodifiableMap(subToBase),
            Collections.unmodifiableSet(basesWithKeywordSub),
            Collections.unmodifiableList(take),
            sourceColumnCount,
            pkIndex,
            new FieldInfos(infos.toArray(new FieldInfo[0]))
        );
    }

    private static FieldInfo fieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,
            true,
            false,
            IndexOptions.NONE,
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    /**
     * Assign a column kind based on its Arrow field. Returns {@code null}
     * for columns that are not surfaced (unsigned integers, vectors,
     * structs, decimals etc.). {@code Utf8} columns are returned as
     * {@link ColumnKind#TEXT_FTS} here and refined to
     * {@link ColumnKind#TEXT_KEYWORD} by {@link #derive} once it knows
     * whether the column carries an FTS index.
     */
    private static ColumnKind classify(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int intType && intType.getIsSigned()) {
            return ColumnKind.NUMERIC;
        }
        if (type instanceof ArrowType.Bool) {
            return ColumnKind.BOOLEAN;
        }
        if (type instanceof ArrowType.Utf8) {
            return ColumnKind.TEXT_FTS; // caller may refine to TEXT_KEYWORD
        }
        if (type instanceof ArrowType.Date || type instanceof ArrowType.Timestamp) {
            return ColumnKind.NUMERIC;
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            // Float16 stays unsurfaced: neither the OpenSearch half_float
            // field type nor the Lance Java SDK's Float2Vector round-trip
            // is wired through the reader.
            if (fp.getPrecision() == FloatingPointPrecision.SINGLE || fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return ColumnKind.NUMERIC;
            }
            return null;
        }
        if (type instanceof ArrowType.List
            && field.getChildren().size() == 1
            && field.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
            return ColumnKind.KEYWORD_ARRAY;
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
            return ColumnKind.BINARY;
        }
        return null;
    }

    /** Primary key column name, or empty when the table declares none. */
    String fieldName() {
        return fieldName;
    }

    /** Arrow type family of the primary key ({@code NONE} when {@link #fieldName()} is empty). */
    LancePrimaryKeyType pkType() {
        return pkType;
    }

    /** Surfaced columns in schema order and their kind. Not to be modified. */
    LinkedHashMap<String, ColumnKind> columnKind() {
        return columnKind;
    }

    /** Whether {@code column} is served as numeric doc values (NUMERIC or BOOLEAN). */
    boolean isNumericOrBoolean(String column) {
        ColumnKind kind = columnKind.get(column);
        return kind == ColumnKind.NUMERIC || kind == ColumnKind.BOOLEAN;
    }

    /** Whether {@code column} is a BOOLEAN column. */
    boolean isBoolean(String column) {
        return columnKind.get(column) == ColumnKind.BOOLEAN;
    }

    /** Precision of Float32 / Float64 columns; absent means INTEGER. */
    Map<String, NumericPrecision> numericPrecision() {
        return numericPrecision;
    }

    /** Sub-field name → base column name for keyword multi-fields. */
    Map<String, String> keywordSubFields() {
        return keywordSubFields;
    }

    /** Base column names that carry at least one keyword sub-field. */
    Set<String> basesWithKeywordSub() {
        return basesWithKeywordSub;
    }

    /** Projection of the per-hit row take, in {@code _source} order. */
    List<String> takeColumns() {
        return takeColumns;
    }

    /** How many leading entries of {@link #takeColumns()} are surfaced columns. */
    int sourceColumnCount() {
        return sourceColumnCount;
    }

    /** Index of the primary key inside {@link #takeColumns()}, or -1 when there is no PK. */
    int pkTakeIndex() {
        return pkTakeIndex;
    }

    /** Lucene field infos of every leaf over this schema. */
    public FieldInfos fieldInfos() {
        return fieldInfos;
    }
}
