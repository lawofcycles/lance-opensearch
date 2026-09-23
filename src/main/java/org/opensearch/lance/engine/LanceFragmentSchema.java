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
import java.util.LinkedHashSet;
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
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;

/**
 * What a {@link LanceFragmentLeafReader} knows about its table before it
 * reads any row: which Arrow columns are surfaced and as what kind, the
 * primary key, the multi-field sub-fields, the projection of the per-hit
 * row take and the Lucene {@link FieldInfos}. All of it is a function of
 * the Lance schema at one manifest version plus the index settings
 * (primary key, mapping overrides), so it is derived once and shared by every
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
        BINARY,        // Binary / LargeBinary — FieldInfo only, values fetched for _source
        GEO_POINT      // geo_point override on Struct<Float64,Float64> or FixedSizeList<Float64>[2] — SortedNumericDocValues of encoded
                       // lat|lon longs
    }

    /**
     * How a {@link ColumnKind#GEO_POINT} column stores its point. A
     * Struct column names its two Float64 children ({@code latChild} /
     * {@code lonChild}, resolved from the accepted name pairs at derive
     * time); a FixedSizeList column carries {@code null} child names and
     * {@code latFirst} decides which of the two elements is the
     * latitude (the attach body's {@code overrides.<col>.order}).
     */
    record GeoPointColumn(String latChild, String lonChild, boolean latFirst) {
        boolean isStruct() {
            return latChild != null;
        }
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
    private final Set<String> ipColumns;
    private final Map<String, GeoPointColumn> geoPointColumns;
    private final Set<String> structColumns;
    private final Set<String> nestedColumns;
    private final Map<String, String> nestedChildToParent;
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
        Set<String> ipColumns,
        Map<String, GeoPointColumn> geoPointColumns,
        Set<String> structColumns,
        Set<String> nestedColumns,
        Map<String, String> nestedChildToParent,
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
        this.ipColumns = ipColumns;
        this.geoPointColumns = geoPointColumns;
        this.structColumns = structColumns;
        this.nestedColumns = nestedColumns;
        this.nestedChildToParent = nestedChildToParent;
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
     * @param overrides   per-column mapping overrides from the index
     *                    settings, nullable; carries the keyword
     *                    sub-fields and the {@code type: keyword}
     *                    overrides that force a Utf8 column with an FTS
     *                    index onto the doc-values path
     * @param ftsColumns  Utf8 columns with an FTS index, from
     *                    {@link #resolveFtsColumns}
     */
    public static LanceFragmentSchema derive(
        Dataset dataset,
        String intField,
        LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        Set<String> ftsColumns
    ) throws IOException {
        // Empty field name overrides pkType regardless of what the caller
        // passed in, so every accessor agrees on "no PK" without the
        // caller having to zip the two settings.
        LancePrimaryKeyType effectivePkType = intField.isEmpty() ? LancePrimaryKeyType.NONE : pkType;
        Map<String, LinkedHashMap<String, String>> multiFields = overrides == null ? Map.of() : overrides.subFields();
        // A `type: keyword` override maps the column to keyword, so the
        // reader must serve it through SortedSetDocValues like any other
        // keyword column; drop it from the FTS set before classification
        // refines TEXT_FTS. The Lance inverted index on the column stays
        // in place, it is just not consulted by this index. A `wildcard`
        // override is served by the same keyword path (only the mapping
        // meta differs), so it joins the same set. An `ip` override does
        // the same and additionally marks the column so the dictionary
        // loaders encode every value into the 16 byte InetAddressPoint
        // form the `ip` field type reads.
        Set<String> keywordOverridden = new HashSet<>();
        if (overrides != null) {
            keywordOverridden.addAll(overrides.keywordColumns());
            keywordOverridden.addAll(overrides.wildcardColumns());
        }
        Set<String> ipOverridden = overrides == null ? Set.of() : overrides.ipColumns();
        // A `type: geo_point` override classifies its Struct or
        // FixedSizeList column as GEO_POINT: one field, encoded lat|lon
        // longs served through SortedNumericDocValues. The map value is
        // the declared FixedSizeList order (null on a Struct, whose
        // child names fix the order).
        Map<String, String> geoOverridden = overrides == null ? Map.of() : overrides.geoPointColumns();

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
        // ftsColumns set rather than a per-leaf JNI call. Struct columns
        // contribute their supported children under dotted paths
        // (parent.child), recursing into nested structs; the parent name
        // itself carries no doc values but joins the row take so _source
        // can render the object. topLevelOrder keeps the take projection
        // in schema order: surfaced scalar columns by their own name,
        // struct columns by the parent name.
        LinkedHashMap<String, ColumnKind> columnKind = new LinkedHashMap<>();
        Map<String, NumericPrecision> numericPrecision = new LinkedHashMap<>();
        Map<String, GeoPointColumn> geoPointColumns = new LinkedHashMap<>();
        Set<String> structColumns = new LinkedHashSet<>();
        Set<String> nestedColumns = new LinkedHashSet<>();
        Map<String, String> nestedChildToParent = new LinkedHashMap<>();
        List<String> topLevelOrder = new ArrayList<>();
        try {
            // A text_analyzer override hides its derived tokens column
            // from the reader (no FieldInfo, no _source entry: queries
            // reach it through Lance directly) and serves the base
            // column like any other lance_text column (FieldInfo only)
            // once the derived column exists. While the backfill is
            // pending the base keeps its default classification,
            // matching the mapping derivation's fallback.
            Set<String> derivedTokensColumns = new HashSet<>();
            Set<String> analyzerModeBases = new HashSet<>();
            if (overrides != null && !overrides.textAnalyzerColumns().isEmpty()) {
                Set<String> utf8Columns = new HashSet<>();
                for (Field field : dataset.getSchema().getFields()) {
                    if (field.getType() instanceof ArrowType.Utf8) {
                        utf8Columns.add(field.getName());
                    }
                }
                for (Map.Entry<String, LanceOverrides.Column> entry : overrides.textAnalyzerColumns().entrySet()) {
                    String derived = LanceOverrides.derivedColumnName(entry.getKey(), entry.getValue());
                    if (utf8Columns.contains(derived)) {
                        derivedTokensColumns.add(derived);
                        if (utf8Columns.contains(entry.getKey())) {
                            analyzerModeBases.add(entry.getKey());
                        }
                    }
                }
            }
            for (Field field : dataset.getSchema().getFields()) {
                if (derivedTokensColumns.contains(field.getName())) {
                    continue;
                }
                if (geoOverridden.containsKey(field.getName())) {
                    // Geo override first: it replaces both the object
                    // classification a Struct would get and the null a
                    // FixedSizeList would get. A column whose current
                    // shape no longer admits the override (a schema
                    // reset since attach) falls through to the normal
                    // classification, mirroring the lenient derive.
                    GeoPointColumn spec = geoPointSpec(field, geoOverridden.get(field.getName()));
                    if (spec != null) {
                        columnKind.put(field.getName(), ColumnKind.GEO_POINT);
                        geoPointColumns.put(field.getName(), spec);
                        topLevelOrder.add(field.getName());
                        continue;
                    }
                }
                if (field.getType() instanceof ArrowType.Struct) {
                    int before = columnKind.size();
                    classifyStructChildren(field, field.getName(), columnKind, numericPrecision);
                    if (columnKind.size() > before) {
                        structColumns.add(field.getName());
                        topLevelOrder.add(field.getName());
                    }
                    continue;
                }
                if (isListOfStruct(field)) {
                    // List<Struct> surfaces as a nested field: every list
                    // element becomes a hidden child doc of the row's
                    // parent doc (see NestedDocLayout), so the children's
                    // doc values live on the child docs, not on the row.
                    int before = columnKind.size();
                    classifyNestedChildren(field.getChildren().get(0), field.getName(), columnKind, numericPrecision);
                    if (columnKind.size() > before) {
                        nestedColumns.add(field.getName());
                        topLevelOrder.add(field.getName());
                        for (String path : columnKind.keySet()) {
                            if (path.startsWith(field.getName() + ".") && !nestedChildToParent.containsKey(path)) {
                                nestedChildToParent.put(path, field.getName());
                            }
                        }
                    }
                    continue;
                }
                ColumnKind kind = classify(field);
                if (kind == null) {
                    continue;
                }
                if (kind == ColumnKind.TEXT_FTS) {
                    kind = (ftsColumns.contains(field.getName()) || analyzerModeBases.contains(field.getName()))
                        && !keywordOverridden.contains(field.getName())
                        && !ipOverridden.contains(field.getName()) ? ColumnKind.TEXT_FTS : ColumnKind.TEXT_KEYWORD;
                }
                columnKind.put(field.getName(), kind);
                topLevelOrder.add(field.getName());
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
            if (columnKind.putIfAbsent(intField, ColumnKind.NUMERIC) == null) {
                topLevelOrder.add(intField);
            }
        }

        // Projection for the per-hit row take: every surfaced top-level
        // column in schema order (struct columns project the whole parent
        // so _source renders the object with its children), plus the PK
        // column appended when its Arrow type is one classify() declines
        // (the take still needs it for _id).
        List<String> take = new ArrayList<>(topLevelOrder);
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
        // drop them by name. Struct children appear under their dotted
        // path; the struct parent itself has no FieldInfo (an object
        // field has no doc values of its own). Multi-fields append a
        // synthetic SORTED_SET entry per keyword sub-field; the data
        // lives on the base column.
        List<FieldInfo> infos = new ArrayList<>();
        int number = 10;
        for (Map.Entry<String, ColumnKind> entry : columnKind.entrySet()) {
            DocValuesType dvType = switch (entry.getValue()) {
                case NUMERIC, BOOLEAN -> DocValuesType.NUMERIC;
                case TEXT_KEYWORD, KEYWORD_ARRAY -> DocValuesType.SORTED_SET;
                case GEO_POINT -> DocValuesType.SORTED_NUMERIC;
                case TEXT_FTS, BINARY -> DocValuesType.NONE;
            };
            infos.add(fieldInfo(entry.getKey(), number++, dvType));
        }
        for (String subName : subToBase.keySet()) {
            infos.add(fieldInfo(subName, number++, DocValuesType.SORTED_SET));
        }
        if (!nestedColumns.isEmpty()) {
            // The two fields OpenSearch's nested machinery reads. The
            // parent filter (Queries.newNonNestedFilter) is a
            // FieldExistsQuery on _primary_term, answered by numeric doc
            // values present on parent docs only; the child filter
            // (NestedPathFieldMapper.filter) is a TermQuery on
            // _nested_path, answered by postings whose one term per
            // nested column matches that column's child docs.
            infos.add(fieldInfo(SeqNoFieldMapper.PRIMARY_TERM_NAME, number++, DocValuesType.NUMERIC));
            infos.add(indexedFieldInfo(NestedPathFieldMapper.NAME, number++));
        }

        // Columns whose dictionary terms are InetAddressPoint-encoded:
        // the `ip` overrides that classified onto an ordinal-based kind
        // (a scalar Utf8 or a List<Utf8> column present in this
        // manifest). An override whose column is absent waits in the
        // index setting and encodes nothing here.
        Set<String> ipColumns = new LinkedHashSet<>();
        for (String ipColumn : ipOverridden) {
            ColumnKind kind = columnKind.get(ipColumn);
            if (kind == ColumnKind.TEXT_KEYWORD || kind == ColumnKind.KEYWORD_ARRAY) {
                ipColumns.add(ipColumn);
            }
        }

        return new LanceFragmentSchema(
            intField,
            effectivePkType,
            columnKind,
            Collections.unmodifiableMap(numericPrecision),
            Collections.unmodifiableMap(subToBase),
            Collections.unmodifiableSet(basesWithKeywordSub),
            Collections.unmodifiableSet(ipColumns),
            Collections.unmodifiableMap(geoPointColumns),
            Collections.unmodifiableSet(structColumns),
            Collections.unmodifiableSet(nestedColumns),
            Collections.unmodifiableMap(nestedChildToParent),
            Collections.unmodifiableList(take),
            sourceColumnCount,
            pkIndex,
            new FieldInfos(infos.toArray(new FieldInfo[0]))
        );
    }

    /**
     * Resolve the storage of a geo_point-overridden column, or
     * {@code null} when the column's current Arrow shape does not admit
     * the override (a schema reset since attach; the caller falls back
     * to the normal classification). Mirrors the attach-time validation
     * in {@code RestAttachAction.validateColumnOverride}: a Struct with
     * two Float64 children named {@code (lat, lon)},
     * {@code (latitude, longitude)} or {@code (y, x)} in either order,
     * or a FixedSizeList&lt;Float64&gt;[2] whose element order comes
     * from the override's declared {@code order} ({@code lat_lon} when
     * none was declared).
     */
    private static GeoPointColumn geoPointSpec(Field field, String declaredOrder) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Struct && field.getChildren().size() == 2) {
            Field c0 = field.getChildren().get(0);
            Field c1 = field.getChildren().get(1);
            boolean bothFloat64 = c0.getType() instanceof ArrowType.FloatingPoint fp0
                && fp0.getPrecision() == FloatingPointPrecision.DOUBLE
                && c1.getType() instanceof ArrowType.FloatingPoint fp1
                && fp1.getPrecision() == FloatingPointPrecision.DOUBLE;
            if (!bothFloat64) {
                return null;
            }
            String n0 = c0.getName().toLowerCase(java.util.Locale.ROOT);
            String n1 = c1.getName().toLowerCase(java.util.Locale.ROOT);
            if (isLatName(n0) && isLonName(n1)) {
                return new GeoPointColumn(c0.getName(), c1.getName(), true);
            }
            if (isLonName(n0) && isLatName(n1)) {
                return new GeoPointColumn(c1.getName(), c0.getName(), true);
            }
            return null;
        }
        if (type instanceof ArrowType.FixedSizeList fsl && fsl.getListSize() == 2 && field.getChildren().size() == 1) {
            ArrowType childType = field.getChildren().get(0).getType();
            if (childType instanceof ArrowType.FloatingPoint fp && fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return new GeoPointColumn(null, null, !"lon_lat".equals(declaredOrder));
            }
        }
        return null;
    }

    private static boolean isLatName(String name) {
        return "lat".equals(name) || "latitude".equals(name) || "y".equals(name);
    }

    private static boolean isLonName(String name) {
        return "lon".equals(name) || "longitude".equals(name) || "x".equals(name);
    }

    /** Whether {@code field} is a {@code List} whose single child is a {@code Struct}. */
    static boolean isListOfStruct(Field field) {
        return field.getType() instanceof ArrowType.List
            && field.getChildren().size() == 1
            && field.getChildren().get(0).getType() instanceof ArrowType.Struct;
    }

    /**
     * Classify the fields of a nested column's element struct into
     * {@code columnKind} under their dotted path, recursing into struct
     * children. The kinds a nested child may take are narrower than a
     * top-level struct child's: only scalars a child doc can carry as a
     * single-valued doc value (NUMERIC, BOOLEAN, TEXT_KEYWORD).
     * {@code List<Utf8>}, Binary and every other type are skipped, and
     * Utf8 always classifies as TEXT_KEYWORD (Lance FTS indexes target
     * top-level columns). {@code List<Struct>} inside an element (nested
     * in nested) is skipped as well.
     */
    private static void classifyNestedChildren(
        Field elementStruct,
        String path,
        LinkedHashMap<String, ColumnKind> columnKind,
        Map<String, NumericPrecision> numericPrecision
    ) {
        for (Field child : elementStruct.getChildren()) {
            String childPath = path + "." + child.getName();
            if (child.getType() instanceof ArrowType.Struct) {
                classifyNestedChildren(child, childPath, columnKind, numericPrecision);
                continue;
            }
            ColumnKind kind = classify(child);
            if (kind == null || kind == ColumnKind.BINARY || kind == ColumnKind.KEYWORD_ARRAY) {
                continue;
            }
            if (kind == ColumnKind.TEXT_FTS) {
                kind = ColumnKind.TEXT_KEYWORD;
            }
            columnKind.put(childPath, kind);
            if (child.getType() instanceof ArrowType.FloatingPoint fp) {
                if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                    numericPrecision.put(childPath, NumericPrecision.FLOAT);
                } else if (fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    numericPrecision.put(childPath, NumericPrecision.DOUBLE);
                }
            }
        }
    }

    /**
     * Classify the children of a Struct column into {@code columnKind}
     * under their dotted path ({@code parent.child}), recursing into
     * nested structs. Children follow the top-level {@link #classify}
     * rules with two exceptions that mirror the mapping derivation:
     * Utf8 children are always {@link ColumnKind#TEXT_KEYWORD} (Lance
     * FTS indexes target top-level columns only) and Binary children
     * are skipped (the mapping does not surface them inside a struct).
     * Unsupported children are simply absent, matching the mapping's
     * "skipped with a note" behaviour.
     */
    private static void classifyStructChildren(
        Field structField,
        String path,
        LinkedHashMap<String, ColumnKind> columnKind,
        Map<String, NumericPrecision> numericPrecision
    ) {
        for (Field child : structField.getChildren()) {
            String childPath = path + "." + child.getName();
            if (child.getType() instanceof ArrowType.Struct) {
                classifyStructChildren(child, childPath, columnKind, numericPrecision);
                continue;
            }
            ColumnKind kind = classify(child);
            if (kind == null || kind == ColumnKind.BINARY) {
                continue;
            }
            if (kind == ColumnKind.TEXT_FTS) {
                kind = ColumnKind.TEXT_KEYWORD;
            }
            columnKind.put(childPath, kind);
            if (child.getType() instanceof ArrowType.FloatingPoint fp) {
                if (fp.getPrecision() == FloatingPointPrecision.SINGLE) {
                    numericPrecision.put(childPath, NumericPrecision.FLOAT);
                } else if (fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
                    numericPrecision.put(childPath, NumericPrecision.DOUBLE);
                }
            }
        }
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
     * A postings-only field ({@code IndexOptions.DOCS}, no doc values),
     * the shape {@code NestedPathFieldMapper} indexes {@code _nested_path}
     * with, so a {@code TermQuery} on it resolves through
     * {@code LeafReader.terms}.
     */
    private static FieldInfo indexedFieldInfo(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            true,
            false,
            IndexOptions.DOCS,
            DocValuesType.NONE,
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
     * whether the column carries an FTS index. Struct columns return
     * {@code null} here because {@link #derive} routes them through
     * {@link #classifyStructChildren} instead.
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

    /**
     * Columns whose dictionary terms are the 16 byte
     * {@link org.apache.lucene.document.InetAddressPoint} encoding of
     * the stored strings ({@code type: ip} overrides). Their keyword
     * sub-fields keep serving the raw strings through a separate
     * per-leaf dictionary.
     */
    Set<String> ipColumns() {
        return ipColumns;
    }

    /**
     * geo_point-overridden columns and how each stores its point
     * (Struct child names or FixedSizeList element order). Keys are the
     * {@link ColumnKind#GEO_POINT} entries of {@link #columnKind()}.
     */
    Map<String, GeoPointColumn> geoPointColumns() {
        return geoPointColumns;
    }

    /**
     * The value encoder the dictionary loaders install for
     * {@code column}, or {@code null} when the raw UTF-8 bytes are the
     * dictionary terms. Keyed by the dictionary's storage name: the
     * base column name answers the encoder, a sub-field name answers
     * {@code null} (its raw view interns unencoded strings).
     */
    KeywordDictionaryBuilder.TermEncoder termEncoder(String column) {
        return ipColumns.contains(column) ? IpTermEncoder.INSTANCE : null;
    }

    /**
     * Top-level Struct column names with at least one surfaced child.
     * These columns appear in {@link #takeColumns()} under the parent
     * name (the take projects the whole struct) while their children
     * appear in {@link #columnKind()} under dotted paths.
     */
    Set<String> structColumns() {
        return structColumns;
    }

    /**
     * Top-level {@code List<Struct>} column names with at least one
     * surfaced child. Their children live in {@link #columnKind()} under
     * dotted paths but are served on nested child docs (see
     * {@link NestedDocLayout}); the parent name joins
     * {@link #takeColumns()} so {@code _source} renders the array.
     */
    public Set<String> nestedColumns() {
        return nestedColumns;
    }

    /** Dotted child path → its nested column, for children of {@link #nestedColumns()}. */
    Map<String, String> nestedChildToParent() {
        return nestedChildToParent;
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
