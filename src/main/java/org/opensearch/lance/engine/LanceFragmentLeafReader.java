/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

/**
 * LeafReader over one Lance fragment.
 *
 * Docids are physical row offsets within the fragment, liveDocs reflects the
 * fragment's deletion file (built from a {@code _rowaddr}-only scan, and only
 * when the fragment metadata says a deletion file exists), and the numeric
 * field is served as DocValues from the Lance column. This is the RFC's
 * "leaf corresponds to a fragment group" contract in its minimal form.
 *
 * <p>Construction reads no data pages unless a deletion file is present.
 * {@code _id} and {@code _source} are fetched per hit through
 * {@link #prefetchRows} / {@link #materialiseStoredFields}, so the cost of
 * opening a leaf does not grow with the number of rows in the fragment.
 */
public final class LanceFragmentLeafReader extends LeafReader {

    /**
     * Column kind resolved once during construction from the fragment schema
     * plus the caller-resolved set of Utf8 columns that carry an FTS index
     * (see {@link #resolveFtsColumns}).
     * The constructor does not materialise column data; each column moves
     * from "declared in schema" to "loaded" the first time a Lucene accessor
     * asks for it, so heap scales with the columns a query touches.
     */
    private enum ColumnKind {
        NUMERIC,       // signed Int / Date / Timestamp / Float32 / Float64 — served as NumericDocValues
        BOOLEAN,       // Bool — served as NumericDocValues via ConcurrentHashMap
        TEXT_FTS,      // Utf8 with a Lance FTS index — FieldInfo only, no doc values
        TEXT_KEYWORD,  // Utf8 without an FTS index — SortedSetDocValues via ords
        KEYWORD_ARRAY, // List<Utf8> — multi-valued SortedSetDocValues
        BINARY         // Binary / LargeBinary — FieldInfo only, values fetched for _source
    }

    /**
     * Encoding of a value stored inside {@code numericColumns}. Every
     * numeric column shares the same {@code long[]} storage so that
     * {@link org.apache.lucene.index.NumericDocValues} can hand back a
     * {@code long} per doc regardless of the underlying Arrow type. The
     * enum records how to render that {@code long} back to a JSON value
     * inside {@link #materialiseStoredFields} and — where relevant — how
     * downstream OpenSearch field mappers decode the value on read.
     *
     * <ul>
     *   <li>{@link #INTEGER} — a plain integer (default). Emitted as a
     *       {@code long} in {@code _source}. Signed integer columns and
     *       date / timestamp columns fall in here; the epoch-millis
     *       normalisation done inside {@link #readAsLong} means the
     *       {@code DateFieldMapper} decodes them directly.</li>
     *   <li>{@link #FLOAT} — a {@code float32} column whose values are
     *       stored as {@code NumericUtils.floatToSortableInt} results
     *       widened to {@code long}. OpenSearch's {@code float} field
     *       type decodes the stored value via
     *       {@code NumericUtils.sortableIntToFloat((int) longValue)}
     *       inside {@code SortedNumericDoubleValues}, so range / sort /
     *       aggregation work through the numeric doc value path
     *       unchanged. {@code _source} rendering decodes the same way
     *       so the JSON value matches the original {@code float}.</li>
     *   <li>{@link #DOUBLE} — a {@code float64} column, mirrored to
     *       {@code NumericUtils.doubleToSortableLong} on the write side
     *       and {@code sortableLongToDouble} on the read side.</li>
     * </ul>
     */
    private enum NumericPrecision {
        INTEGER,
        FLOAT,
        DOUBLE
    }

    private final String fieldName;
    /**
     * Arrow type family of the declared primary key. Drives {@code _id}
     * materialisation in {@link #materialiseStoredFields}:
     * {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#LONG}
     * / {@code UNSIGNED_LONG} render the PK value the row take returned
     * as a decimal string, {@code KEYWORD} echoes the Utf8 value
     * verbatim, and
     * {@link org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType#NONE}
     * falls back to a synthesised {@code "<fragment>-<offset>"}. Set to
     * {@code NONE} whenever {@link #fieldName} is empty regardless of what
     * the caller passed in.
     */
    private final org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType;
    private final int maxDoc;
    private final int numDocs;
    /**
     * Rows fetched by {@link #prefetchRows} keyed by doc id (physical
     * row offset within the fragment). Each entry holds the decoded
     * values of {@link #takeColumns} in that order; {@link #MISSING_ROW}
     * marks a doc id the take scan did not return (deleted between the
     * scan that produced the doc id and the fetch, which should not
     * happen because the reader pins one Dataset version, but is
     * handled so a miss does not trigger a second scan for the same
     * doc).
     *
     * <p>Fetching only the hit rows keeps the per-request cost of
     * {@code _id} / {@code _source} proportional to {@code size} rather
     * than to the number of rows in the fragment; a constructor-time
     * {@code _rowaddr + PK} scan would make even a 1-hit query scale
     * with total row count.
     */
    private final Map<Integer, Object[]> takenRows = new ConcurrentHashMap<>();
    private static final Object[] MISSING_ROW = new Object[0];
    /**
     * Upper bound on how many row addresses one {@code _rowaddr IN
     * (...)} take scan carries. Lance turns the IN list into a direct
     * take by address (no filter evaluation), so the cap only bounds
     * the SQL string the JNI layer has to parse. {@code
     * index.max_result_window} defaults to 10000, so a normal fetch
     * needs at most three chunks.
     */
    private static final int TAKE_CHUNK = 4096;
    /**
     * Column names the row take projects, in {@code _source} emission
     * order. The first {@link #sourceColumnCount} entries are the
     * surfaced columns from {@link #columnKind} (schema order); when
     * the primary key column is not itself surfaced (its Arrow type is
     * one {@link #classify} declines) it is appended after them so
     * {@code _id} can still be rendered.
     */
    private final List<String> takeColumns;
    private final int sourceColumnCount;
    /**
     * Index of the primary key column inside {@link #takeColumns}, or
     * {@code -1} when {@link #pkType} is {@code NONE}.
     */
    private final int pkTakeIndex;
    /**
     * Sub-field name → base column name lookup for multi-fields. Empty
     * when the attach body did not declare {@code multi_fields}. Every
     * entry is a keyword sub-field on a Utf8 base column; the reader
     * routes {@link #getSortedDocValues} / {@link #getSortedSetDocValues}
     * on the sub-field name through the base column's ord data
     * structure; a keyword sub-field of a Utf8 column holds the same
     * values as the column, so a second ord map would be a copy.
     */
    private final java.util.Map<String, String> keywordSubFields;
    /**
     * Base column names that carry at least one keyword sub-field.
     * {@link #ensureTextLoaded} consults this set so a TEXT_FTS column
     * with a sub-field still builds the ord data structure that
     * TEXT_KEYWORD would build by default.
     */
    private final java.util.Set<String> basesWithKeywordSub;
    private final Bits liveDocs;
    private final FieldInfos fieldInfos;
    private final Dataset dataset;
    private final int fragmentId;
    /**
     * SQL predicate the caller wants applied to every per-column scan
     * this reader issues, or {@code null} for an unfiltered scan.
     *
     * <p>Fragment path queries whose top-level shape is a scalar
     * filter that {@code LanceKnnFilterTranslator} can translate to
     * Lance SQL ship the translated predicate as
     * {@code LanceFragmentQueryRequest.filterSql()}, and the fragment
     * dispatch handler forwards it here so the lazy column loads
     * inside {@link #ensureNumericLoaded} et al. only materialise the
     * rows that match. Without the predicate, {@code filter + terms}
     * or {@code filter + sum} reads every value in the aggregated
     * column even though the Weight already filters.
     *
     * <p>Kept {@code null} when the query cannot be expressed in
     * Lance SQL (FTS, knn, unsupported shapes) so the reader falls
     * back to full-column scans and the aggregator still runs on
     * every doc the Weight yields.
     */
    private final String filterSql;
    // Column kind resolved eagerly during construction. Preserves schema order
    // so materialiseStoredFields emits _source keys in schema order regardless
    // of which columns have been loaded so far.
    private final LinkedHashMap<String, ColumnKind> columnKind = new LinkedHashMap<>();
    /**
     * Precision override for numeric columns whose {@link ColumnKind} is
     * {@link ColumnKind#NUMERIC} but whose underlying Arrow type is not
     * a plain integer or a date/timestamp. Populated during the schema
     * pass for {@code Float32} and {@code Float64} columns (see
     * {@link NumericPrecision}); every other numeric column is absent
     * from the map and defaults to {@link NumericPrecision#INTEGER}
     * inside the {@code getOrDefault} lookups.
     */
    private final Map<String, NumericPrecision> numericPrecision = new ConcurrentHashMap<>();
    // Per-column monitors so ensureXxxLoaded serialises the Lance scan for
    // that column without blocking other columns. The first accessor pays the
    // scan cost, subsequent readers see the populated map entry via the
    // ConcurrentHashMap happens-before edge.
    private final Map<String, Object> columnLocks = new ConcurrentHashMap<>();
    // Per-column data. Populated lazily by ensureXxxLoaded; ConcurrentHashMap
    // provides the visibility guarantee for the writer / reader pair.
    private final Map<String, long[]> numericColumns = new ConcurrentHashMap<>();
    // Per-column presence bitmap; bit set = value present, bit clear = Arrow null.
    // NumericDocValues.advanceExact and _source materialisation both consult this
    // to distinguish "value is 0" from "value is missing" for nullable Arrow columns.
    private final Map<String, FixedBitSet> numericPresence = new ConcurrentHashMap<>();
    private final Map<String, long[]> booleanColumns = new ConcurrentHashMap<>();
    private final Map<String, FixedBitSet> booleanPresence = new ConcurrentHashMap<>();
    // Utf8 columns without an FTS index surface as keyword. Their sorted
    // term dictionary plus per-doc ordinals let getSortedDocValues serve
    // term, terms, aggregation and sort requests through the doc value
    // path. No per-row String copy is kept: _source is rendered from the
    // per-hit take, and Binary columns have no doc value representation
    // at all so they are never loaded through the reader.
    private final Map<String, BytesRef[]> keywordTerms = new ConcurrentHashMap<>();
    private final Map<String, int[]> keywordOrds = new ConcurrentHashMap<>();
    // List<Utf8> columns surface as multi-valued keyword; keywordArrayOrds
    // and keywordArrayTerms back a multi-valued SortedSetDocValues.
    private final Map<String, int[][]> keywordArrayOrds = new ConcurrentHashMap<>();
    private final Map<String, BytesRef[]> keywordArrayTerms = new ConcurrentHashMap<>();

    /**
     * Shard-level column materialisation coordinator. Non-null when the
     * containing {@link LanceDirectoryReader} was created with a
     * {@link LanceShardColumnCache} (the fragment-dispatch and
     * whole-table open paths both install one). When set,
     * {@link #ensureNumericLoaded} delegates the actual scan work to
     * the cache so the newScan overhead is paid once per column
     * across the reader instead of once per (column, leaf). When
     * unset the leaf falls back to its own per-fragment scan for
     * backwards compatibility (unit tests that construct leaves
     * directly, callers that skip {@link LanceDirectoryReader}).
     */
    private volatile LanceShardColumnCache shardColumnCache;

    // Bridge to Lucene's cache lifecycle. IndicesQueryCache, IndicesFieldDataCache
    // and IndicesRequestCache all key entries by IndexReader.CacheKey and rely on
    // IndexReader.ClosedListener to invalidate them. IndexReader.CacheKey has a
    // package-private constructor, so a plugin sitting outside the
    // org.apache.lucene.index package cannot mint its own key. We instead hold a
    // tiny one-doc Lucene reader whose lifetime is bound to this fragment reader:
    // its CacheHelper is exposed as ours, and closing this reader closes the
    // bridge, which fires the listeners registered by the OpenSearch caches.
    private final DirectoryReader cacheLifetimeBridge;

    /**
     * Resolve which Utf8 columns of {@code dataset} carry an FTS
     * (inverted) index. The result feeds every leaf's schema pass so
     * the {@code describeIndices} round trip happens once per
     * {@link LanceDirectoryReader} open instead of once per (leaf,
     * Utf8 column) pair. On a table with 80 fragments and two text
     * columns that is 160 JNI calls saved per request.
     *
     * @return set of column names that have an index supporting FTS;
     *         never {@code null}, possibly empty
     */
    static java.util.Set<String> resolveFtsColumns(Dataset dataset) throws IOException {
        java.util.Set<String> fts = new java.util.HashSet<>();
        try {
            for (org.apache.arrow.vector.types.pojo.Field field : dataset.getSchema().getFields()) {
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
     * Open a leaf over one Lance fragment.
     *
     * <p>Construction is metadata-only unless the fragment carries a
     * deletion file. The schema pass classifies columns from
     * {@code dataset.getSchema()} and the caller-supplied
     * {@code ftsColumns}; no data pages are read. When
     * {@code hasDeletionFile} is true a single {@code _rowaddr}-only
     * scan of this fragment builds {@link #liveDocs} (Lucene needs the
     * bitmap because {@link org.apache.lucene.search.MatchAllDocsQuery}
     * and doc value iterators walk {@code 0..maxDoc} without going
     * through a Lance scan that would skip deleted rows). Fragments
     * without a deletion file report every physical row as live and
     * skip the scan entirely. Primary key values are not read here;
     * {@link #materialiseStoredFields} fetches them per hit through
     * {@link #prefetchRows}.
     *
     * @param dataset         the Lance dataset; the leaf does not take
     *                        ownership
     * @param fragmentId      Lance fragment id this leaf exposes
     * @param physicalRows    {@code FragmentMetadata.getPhysicalRows()},
     *                        which becomes {@link #maxDoc}
     * @param hasDeletionFile {@code FragmentMetadata.getDeletionFile() != null}
     * @param intField        primary key column name, or empty when
     *                        the table declares no primary key
     * @param pkType          Arrow type family of the primary key
     * @param multiFields     attach-body multi-fields spec, nullable
     * @param ftsColumns      Utf8 columns with an FTS index, from
     *                        {@link #resolveFtsColumns}
     * @param filterSql       Lance SQL predicate every per-column scan
     *                        this leaf issues layers into its
     *                        {@link ScanOptions#filter}, or {@code null}
     *                        for unfiltered scans (see {@link #filterSql})
     */
    public LanceFragmentLeafReader(
        Dataset dataset,
        int fragmentId,
        long physicalRows,
        boolean hasDeletionFile,
        String intField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        java.util.Set<String> ftsColumns,
        String filterSql
    ) throws IOException {
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.fieldName = intField;
        // Empty field name overrides pkType regardless of what the caller
        // passed in. The engine performs the same override at setting-read
        // time, but the reader is also invoked from
        // LanceDirectoryReader.openForFragments where the caller may pass
        // a mismatched pair; canonicalising here keeps every accessor
        // agreeing on "no PK" without needing the caller to zip them.
        this.pkType = intField.isEmpty() ? org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.NONE : pkType;
        this.maxDoc = (int) physicalRows;
        this.filterSql = filterSql;
        // Flatten the multi-fields spec into "<sub>" → "<base>" lookup so
        // getSortedDocValues("body.raw") can route to the base column's
        // ord data structure without re-parsing the spec. Only keyword
        // sub-fields are supported today, so any
        // sub-field type that is not "keyword" is skipped defensively
        // rather than errored out — the attach-time validation is where
        // the error surfaces.
        java.util.LinkedHashMap<String, String> subToBase = new java.util.LinkedHashMap<>();
        java.util.Set<String> basesWithKeywordSub = new java.util.HashSet<>();
        if (multiFields != null && !multiFields.isEmpty()) {
            for (java.util.Map.Entry<String, java.util.LinkedHashMap<String, String>> entry : multiFields.entrySet()) {
                String baseName = entry.getKey();
                for (java.util.Map.Entry<String, String> sub : entry.getValue().entrySet()) {
                    if (!"keyword".equals(sub.getValue())) {
                        continue;
                    }
                    subToBase.put(baseName + "." + sub.getKey(), baseName);
                    basesWithKeywordSub.add(baseName);
                }
            }
        }
        this.keywordSubFields = java.util.Collections.unmodifiableMap(subToBase);
        this.basesWithKeywordSub = java.util.Collections.unmodifiableSet(basesWithKeywordSub);

        // Schema pass: classify every column we might surface. FTS
        // presence for Utf8 columns comes from the caller-resolved
        // ftsColumns set (one describeIndices sweep per reader, see
        // resolveFtsColumns) rather than a per-leaf JNI call. This is
        // metadata only — no data pages are read here.
        try {
            for (org.apache.arrow.vector.types.pojo.Field field : dataset.getSchema().getFields()) {
                ColumnKind kind = classify(field);
                if (kind == null) {
                    continue;
                }
                if (kind == ColumnKind.TEXT_FTS) {
                    kind = ftsColumns.contains(field.getName()) ? ColumnKind.TEXT_FTS : ColumnKind.TEXT_KEYWORD;
                }
                columnKind.put(field.getName(), kind);
                // Remember the precision of Float32 / Float64 columns
                // so materialiseStoredFields and readAsLong can round
                // trip them through the shared long[] storage. All
                // other numeric columns default to INTEGER via
                // getOrDefault and no entry is written here.
                if (field.getType() instanceof ArrowType.FloatingPoint fp) {
                    if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                        numericPrecision.put(field.getName(), NumericPrecision.FLOAT);
                    } else if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) {
                        numericPrecision.put(field.getName(), NumericPrecision.DOUBLE);
                    }
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        // An UNSIGNED_LONG primary key sits on a Lance UInt64 column that
        // classify() otherwise refuses (unsigned integers are not
        // surfaced by default). Force it into NUMERIC here so the PK
        // gets a NumericDocValues entry alongside signed integer
        // columns; readAsLong already returns the unsigned bit pattern
        // for UInt8Vector, so the doc value path is otherwise
        // untouched. Only this one column is elevated; other UInt64
        // columns stay unsurfaced.
        if (this.pkType == org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.UNSIGNED_LONG && !intField.isEmpty()) {
            columnKind.putIfAbsent(intField, ColumnKind.NUMERIC);
        }

        // Projection for the per-hit row take (see prefetchRows): every
        // surfaced column in schema order so _source keys come out in a
        // stable order, plus the PK column appended when its Arrow type
        // is one classify() declines (the take still needs it for _id).
        List<String> take = new java.util.ArrayList<>(columnKind.keySet());
        this.sourceColumnCount = take.size();
        int pkIndex = -1;
        if (this.pkType != org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.NONE) {
            pkIndex = take.indexOf(intField);
            if (pkIndex < 0) {
                take.add(intField);
                pkIndex = take.size() - 1;
            }
        }
        this.pkTakeIndex = pkIndex;
        this.takeColumns = Collections.unmodifiableList(take);

        // liveDocs: Lucene's MatchAllDocsQuery and the doc value
        // iterators walk 0..maxDoc directly, so deleted physical rows
        // must be masked here. Fragments without a deletion file have
        // no deleted rows and skip the scan; fragments with one run a
        // _rowaddr-only scan (8 bytes per row, no payload columns) to
        // learn which offsets survive. Lance-driven scans (FTS, knn,
        // scalar filter) already skip deleted rows on their own, so
        // this bitmap only has to cover the Lucene-driven iteration.
        if (hasDeletionFile) {
            FixedBitSet live = new FixedBitSet(maxDoc);
            int liveCount = 0;
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(Collections.emptyList())
                .withRowAddress(true)
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        live.set(offset);
                        liveCount++;
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            this.numDocs = liveCount;
            this.liveDocs = liveCount == maxDoc ? null : live;
        } else {
            this.numDocs = maxDoc;
            this.liveDocs = null;
        }

        // FieldInfos derived from columnKind. Order matches the schema pass
        // above; field numbers start at 10 to leave 1 / 2 free for _id and
        // _source (see storedOnly). One entry per column: NUMERIC / BOOLEAN
        // → NumericDocValues, TEXT_KEYWORD / KEYWORD_ARRAY → SortedSet,
        // TEXT_FTS / BINARY → no doc values but the FieldInfo exists so the
        // security plugin's FLS wrapper can drop them by name (see LanceFtsQuery
        // FLS-bypass check; see e21bf3c for the original bug).
        //
        // Multi-fields append a synthetic SORTED_SET entry per keyword
        // sub-field so getSortedDocValues / getSortedSetDocValues on the
        // sub-field name resolve, and so FLS field enumeration sees them.
        // The underlying data lives on the base column; the sub-field
        // entry only exists in the FieldInfos, not in columnKind.
        List<FieldInfo> infos = new java.util.ArrayList<>();
        int number = 10;
        for (Map.Entry<String, ColumnKind> entry : columnKind.entrySet()) {
            DocValuesType dvType = switch (entry.getValue()) {
                case NUMERIC, BOOLEAN -> DocValuesType.NUMERIC;
                case TEXT_KEYWORD, KEYWORD_ARRAY -> DocValuesType.SORTED_SET;
                case TEXT_FTS, BINARY -> DocValuesType.NONE;
            };
            infos.add(
                new FieldInfo(
                    entry.getKey(),
                    number++,
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
                )
            );
        }
        for (String subName : keywordSubFields.keySet()) {
            infos.add(
                new FieldInfo(
                    subName,
                    number++,
                    false,
                    true,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.SORTED_SET,
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
                )
            );
        }
        this.fieldInfos = new FieldInfos(infos.toArray(new FieldInfo[0]));

        ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        this.cacheLifetimeBridge = DirectoryReader.open(bridgeDir);
    }

    /**
     * Assign a column kind based on its Arrow field. Returns {@code null} for
     * columns we do not surface (unsigned integers, vectors, structs, decimals
     * etc.). {@code Utf8} columns are returned as {@link ColumnKind#TEXT_FTS}
     * here and refined to {@link ColumnKind#TEXT_KEYWORD} by the caller after
     * a {@code describeIndices} call — the caller can only issue the FTS check
     * once it knows the column is a Utf8 column, so it does the refinement.
     */
    private static ColumnKind classify(org.apache.arrow.vector.types.pojo.Field field) {
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
            // Float16 stays unsurfaced today: neither the OpenSearch
            // `half_float` field type nor the Lance Java SDK's
            // Float2Vector round-trip is wired through the reader.
            // Float32 / Float64 both fold into the shared numeric doc
            // value path; the precision is remembered separately in
            // numericPrecision so _source and readAsLong can encode /
            // decode via NumericUtils.floatToSortableInt or
            // doubleToSortableLong.
            if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                || fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) {
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

    private Object columnLock(String name) {
        return columnLocks.computeIfAbsent(name, k -> new Object());
    }

    /**
     * Build the {@link ScanOptions} the per-column
     * {@code ensureXxxLoaded} helpers share: pin to this fragment,
     * project a single column, ask for row addresses so the caller
     * can splice the values back into the offset-indexed arrays,
     * and layer {@link #filterSql} into
     * {@link ScanOptions.Builder#filter} when the top-level query
     * pushed one down.
     *
     * <p>Consolidating the option assembly here means every column
     * loader agrees on the same shape; the filter push-down would
     * otherwise be six near-identical edits that could drift.
     */
    private ScanOptions singleColumnScan(String name) {
        ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
            .columns(Collections.singletonList(name))
            .withRowAddress(true);
        if (filterSql != null) {
            builder = builder.filter(filterSql);
        }
        return builder.build();
    }

    /**
     * Lance-scan the single column identified by {@code name} into
     * {@code numericColumns} / {@code numericPresence}. Only the caller that
     * wins the {@link #columnLock} does the scan; others block briefly and
     * then see the populated maps via ConcurrentHashMap's happens-before.
     */
    private void ensureNumericLoaded(String name) throws IOException {
        if (numericColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null) {
            // Delegate to the shard-level coordinator: one scan for
            // the whole reader instead of one per leaf. After the
            // cache returns, publishNumericColumn below has put the
            // fragment's slice into this leaf's numericColumns /
            // numericPresence maps, so the containsKey short-circuit
            // fires on subsequent calls.
            cache.loadNumericColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (numericColumns.containsKey(name)) {
                return;
            }
            long[] col = new long[maxDoc];
            FixedBitSet presence = new FixedBitSet(maxDoc);
            ScanOptions colOptions = singleColumnScan(name);
            try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector vector = root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        if (!vector.isNull(i)) {
                            col[offset] = readAsLong(vector, i);
                            presence.set(offset);
                        }
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            numericPresence.put(name, presence);
            numericColumns.put(name, col);
        }
    }

    /**
     * Called by {@link LanceShardColumnCache#loadNumericColumn} after
     * the shard-level scan has bucketed this leaf's fragment. Simply
     * writes into the per-leaf storage the accessors already read
     * from ({@link #numericColumns}, {@link #numericPresence}), so
     * downstream {@link #getSortedNumericDocValues} calls see the
     * populated arrays through the {@code ConcurrentHashMap}
     * happens-before.
     *
     * <p>Package-private because only the cache should call it — the
     * cache lives alongside this class in
     * {@code org.opensearch.lance.engine} and is the sole owner of
     * the shard-level scan lifecycle.
     */
    void publishNumericColumn(String name, long[] values, FixedBitSet presence) {
        numericPresence.put(name, presence);
        numericColumns.put(name, values);
    }

    /**
     * Attach a {@link LanceShardColumnCache} to this leaf. Called
     * from {@link LanceDirectoryReader}'s open paths after the full
     * leaf list has been assembled so the cache has references to
     * every leaf in the reader. {@code null} disables the cache
     * indirection and returns the leaf to its per-fragment scan
     * fallback (only used by tests that construct leaves without a
     * DirectoryReader).
     */
    void setShardColumnCache(LanceShardColumnCache cache) {
        this.shardColumnCache = cache;
    }

    private void ensureBooleanLoaded(String name) throws IOException {
        if (booleanColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null) {
            cache.loadBooleanColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (booleanColumns.containsKey(name)) {
                return;
            }
            long[] col = new long[maxDoc];
            FixedBitSet presence = new FixedBitSet(maxDoc);
            ScanOptions colOptions = singleColumnScan(name);
            try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    BitVector vector = (BitVector) root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        if (!vector.isNull(i)) {
                            col[offset] = vector.get(i);
                            presence.set(offset);
                        }
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            booleanPresence.put(name, presence);
            booleanColumns.put(name, col);
        }
    }

    /**
     * Publish sink for {@link LanceShardColumnCache#loadBooleanColumn}.
     * Same shape as {@link #publishNumericColumn}; the two boolean
     * bookkeeping maps mirror the numeric ones.
     */
    void publishBooleanColumn(String name, long[] values, FixedBitSet presence) {
        booleanPresence.put(name, presence);
        booleanColumns.put(name, values);
    }

    /**
     * Build the keyword dictionary for a Utf8 column: sorted
     * {@code BytesRef[]} terms plus a per-doc ordinal array (-1 for
     * Arrow null), stored in {@link #keywordTerms} / {@link #keywordOrds}
     * for {@link #getSortedDocValues}. Called only for
     * {@link ColumnKind#TEXT_KEYWORD} columns and for TEXT_FTS columns
     * that carry a {@code multi_fields} keyword sub-field; nothing else
     * reads Utf8 values through the reader any more ({@code _source}
     * comes from the per-hit take), so no per-row {@link String} copy
     * is retained. Delegates to {@link LanceShardColumnCache} when one
     * is installed so the scan runs once per shard.
     */
    private void ensureTextLoaded(String name) throws IOException {
        if (keywordOrds.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null) {
            cache.loadTextColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (keywordOrds.containsKey(name)) {
                return;
            }
            int[] ids = new int[maxDoc];
            Arrays.fill(ids, -1);
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder();
            ScanOptions colOptions = singleColumnScan(name);
            try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    VarCharVector vector = (VarCharVector) root.getVector(name);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (vector.isNull(i)) {
                            continue;
                        }
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        ids[offset] = builder.intern(vector, i);
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            dictionary.remap(ids);
            publishTextColumn(name, dictionary.terms(), ids);
        }
    }

    /**
     * Publish sink for {@link LanceShardColumnCache#loadTextColumn} and
     * the per-leaf fallback above. {@code terms} are sorted in unsigned
     * byte order and {@code ords} maps every doc to an index into
     * {@code terms} or -1. Keyword dictionaries stay per-fragment
     * because {@link org.apache.lucene.index.SortedDocValues}
     * ord-comparison semantics assume per-segment ord spaces.
     */
    void publishTextColumn(String name, BytesRef[] terms, int[] ords) {
        keywordTerms.put(name, terms);
        keywordOrds.put(name, ords);
    }

    /**
     * Build the multi-valued keyword dictionary for a List&lt;Utf8&gt;
     * column: sorted terms plus, per doc, a strictly ascending
     * duplicate-free ordinal array (null for an Arrow-null list), for
     * {@link #getSortedSetDocValues}.
     */
    private void ensureKeywordArrayLoaded(String name) throws IOException {
        if (keywordArrayOrds.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null) {
            cache.loadKeywordArrayColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (keywordArrayOrds.containsKey(name)) {
                return;
            }
            int[][] rows = new int[maxDoc][];
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder();
            ScanOptions colOptions = singleColumnScan(name);
            try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    ListVector vector = (ListVector) root.getVector(name);
                    VarCharVector elements = (VarCharVector) vector.getDataVector();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        if (vector.isNull(i)) {
                            continue;
                        }
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        rows[offset] = internListElements(builder, vector, elements, i);
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            for (int r = 0; r < rows.length; r++) {
                if (rows[r] != null) {
                    rows[r] = dictionary.remapSortedUnique(rows[r]);
                }
            }
            publishKeywordArrayColumn(name, dictionary.terms(), rows);
        }
    }

    /**
     * Intern the non-null elements of list {@code index} and return
     * their insertion-order ids (not yet remapped to sorted ordinals).
     * Shared by the per-leaf loader and {@link LanceShardColumnCache}.
     */
    static int[] internListElements(KeywordDictionaryBuilder builder, ListVector vector, VarCharVector elements, int index) {
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        int[] ids = new int[end - start];
        int count = 0;
        for (int e = start; e < end; e++) {
            if (!elements.isNull(e)) {
                ids[count++] = builder.intern(elements, e);
            }
        }
        return count == ids.length ? ids : Arrays.copyOf(ids, count);
    }

    /**
     * Publish sink for {@link LanceShardColumnCache#loadKeywordArrayColumn}
     * and the per-leaf fallback above. Same contract as
     * {@link #publishTextColumn} but each doc carries an ordinal array
     * ({@code null} for a null list) instead of a scalar ordinal.
     */
    void publishKeywordArrayColumn(String name, BytesRef[] terms, int[][] rowOrds) {
        keywordArrayTerms.put(name, terms);
        keywordArrayOrds.put(name, rowOrds);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        final long[] column;
        final FixedBitSet presence;
        ColumnKind kind = columnKind.get(field);
        if (kind != ColumnKind.NUMERIC && kind != ColumnKind.BOOLEAN) {
            return null;
        }
        try {
            if (kind == ColumnKind.NUMERIC) {
                ensureNumericLoaded(field);
                column = numericColumns.get(field);
                presence = numericPresence.get(field);
            } else {
                ensureBooleanLoaded(field);
                column = booleanColumns.get(field);
                presence = booleanPresence.get(field);
            }
        } catch (IOException e) {
            // LeafReader.getNumericDocValues declares throws IOException but the
            // OpenSearch caller path (SortField.getComparator, doc-value queries)
            // does not, so wrap into UncheckedIOException. In practice this is
            // hit only if the Lance side fails a per-column scan after the
            // constructor has already succeeded.
            throw new UncheckedIOException(e);
        }
        return new NumericDocValues() {
            private int doc = -1;

            @Override
            public long longValue() {
                return column[doc];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (liveDocs != null && !liveDocs.get(target)) {
                    return false;
                }
                // presence bit is clear for Arrow-null docs; exists / term /
                // range / agg / sort all check advanceExact and stop reading
                // the value here.
                return presence.get(target);
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                // Skip liveDocs holes and Arrow-null slots. DocValuesFieldExistsQuery
                // iterates through the doc values with advance/nextDoc alone and does
                // not call advanceExact, so the null bitmap must also be honoured here
                // - otherwise exists / _field_names checks count every row regardless
                // of presence.
                for (int candidate = target; candidate < column.length; candidate++) {
                    if (liveDocs != null && !liveDocs.get(candidate)) {
                        continue;
                    }
                    if (presence.get(candidate)) {
                        doc = candidate;
                        return doc;
                    }
                }
                doc = NO_MORE_DOCS;
                return doc;
            }

            @Override
            public long cost() {
                return column.length;
            }
        };
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        NumericDocValues numeric = getNumericDocValues(field);
        return numeric == null ? null : DocValues.singleton(numeric);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    @Override
    public Bits getLiveDocs() {
        return liveDocs;
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    @Override
    public int maxDoc() {
        return maxDoc;
    }

    @Override
    public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
    }

    @Override
    public Terms terms(String field) {
        return null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        return null;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        // A keyword sub-field (multi-fields) resolves to its base column's
        // ord data structure. Route through the base column name so
        // ensureTextLoaded reuses whatever ords were already built for
        // TEXT_KEYWORD, or builds fresh ords for a TEXT_FTS base that
        // otherwise would not have any.
        String source = keywordSubFields.getOrDefault(field, field);
        ColumnKind kind = columnKind.get(source);
        if (kind != ColumnKind.TEXT_KEYWORD && !basesWithKeywordSub.contains(source)) {
            return null;
        }
        try {
            ensureTextLoaded(source);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int[] ords = keywordOrds.get(source);
        BytesRef[] terms = keywordTerms.get(source);
        if (ords == null || terms == null) {
            return null;
        }
        return keywordSortedDocValues(ords, terms);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        ColumnKind kind = columnKind.get(field);
        if (kind == ColumnKind.KEYWORD_ARRAY) {
            try {
                ensureKeywordArrayLoaded(field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            int[][] arrayOrds = keywordArrayOrds.get(field);
            BytesRef[] arrayTerms = keywordArrayTerms.get(field);
            if (arrayOrds != null && arrayTerms != null) {
                return keywordArraySortedSetDocValues(arrayOrds, arrayTerms);
            }
            return null;
        }
        SortedDocValues single = getSortedDocValues(field);
        return single == null ? null : DocValues.singleton(single);
    }

    private SortedSetDocValues keywordArraySortedSetDocValues(int[][] rowOrds, BytesRef[] terms) {
        return new SortedSetDocValues() {
            private int doc = -1;
            private int[] currentRow;
            private int cursor;

            @Override
            public long nextOrd() {
                // Caller iterates docValueCount() times; no sentinel needed.
                return currentRow[cursor++];
            }

            @Override
            public int docValueCount() {
                return currentRow == null ? 0 : currentRow.length;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return terms[(int) ord];
            }

            @Override
            public long getValueCount() {
                return terms.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                cursor = 0;
                if (liveDocs != null && !liveDocs.get(target)) {
                    currentRow = null;
                    return false;
                }
                currentRow = rowOrds[target];
                return currentRow != null && currentRow.length > 0;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (int i = target; i < rowOrds.length; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && rowOrds[i] != null && rowOrds[i].length > 0) {
                        doc = i;
                        currentRow = rowOrds[i];
                        cursor = 0;
                        return i;
                    }
                }
                doc = NO_MORE_DOCS;
                currentRow = null;
                return doc;
            }

            @Override
            public long cost() {
                return rowOrds.length;
            }
        };
    }

    private SortedDocValues keywordSortedDocValues(int[] ords, BytesRef[] terms) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public int ordValue() {
                return ords[doc];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return terms[ord];
            }

            @Override
            public int getValueCount() {
                return terms.length;
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                if (liveDocs != null && !liveDocs.get(target)) {
                    return false;
                }
                return ords[target] >= 0;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (int i = target; i < ords.length; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && ords[i] >= 0) {
                        doc = i;
                        return i;
                    }
                }
                doc = NO_MORE_DOCS;
                return doc;
            }

            @Override
            public long cost() {
                return ords.length;
            }
        };
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        return null;
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) {
        return null;
    }

    @Override
    public PointValues getPointValues(String field) {
        return null;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) {
        return null;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        return null;
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs) {}

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) {}

    @Override
    public TermVectors termVectors() {
        return TermVectors.EMPTY;
    }

    @Override
    public StoredFields storedFields() {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                materialiseStoredFields(docID, visitor);
            }
        };
    }

    /**
     * Fetch the rows behind {@code docIds} from Lance in one take scan
     * per {@link #TAKE_CHUNK} doc ids and stash them in
     * {@link #takenRows} for {@link #materialiseStoredFields}.
     *
     * <p>The scan filters on {@code _rowaddr IN (...)}. Lance recognises
     * that predicate shape as a take-by-address (see
     * {@code TakeOperation::try_from_expr} in
     * {@code rust/lance/src/dataset/scanner.rs}) and reads exactly the
     * requested rows without evaluating a filter or walking the
     * fragment, so a {@code size:10} fetch touches 10 rows of the
     * projected columns regardless of how many rows the fragment
     * holds. Row addresses are {@code (fragmentId << 32) | docId},
     * which is the same encoding the FTS / knn / scalar-filter scans
     * decode doc ids from, and unlike {@code Dataset.takeRows} it does
     * not depend on whether the table uses stable row ids.
     *
     * <p>Doc ids already present in {@link #takenRows} are skipped so a
     * caller can prefetch a whole page and then let the per-doc
     * {@code document(...)} calls hit the cache. Doc ids the scan does
     * not return are recorded as {@link #MISSING_ROW} so the fallback
     * single-doc prefetch inside {@link #materialiseStoredFields} does
     * not issue a second scan for them.
     *
     * <p>{@link #filterSql} is deliberately not layered in: the doc ids
     * were produced by a scan that already applied it (or by Lucene
     * iteration the caller chose), so re-applying it could only drop
     * rows the caller has decided to return.
     */
    public void prefetchRows(int[] docIds) throws IOException {
        List<Long> addresses = new java.util.ArrayList<>(docIds.length);
        java.util.Set<Integer> requested = new java.util.HashSet<>();
        for (int docId : docIds) {
            if (takenRows.containsKey(docId) || !requested.add(docId)) {
                continue;
            }
            addresses.add(((long) fragmentId << 32) | (docId & 0xFFFFFFFFL));
        }
        if (addresses.isEmpty()) {
            return;
        }
        if (takeColumns.isEmpty()) {
            // Nothing to project (PK-less table whose columns are all
            // unsurfaced): every row renders as a synthesised _id and
            // an empty _source, so there is no reason to call into
            // Lance.
            for (int docId : requested) {
                takenRows.putIfAbsent(docId, new Object[0]);
            }
            return;
        }
        for (int from = 0; from < addresses.size(); from += TAKE_CHUNK) {
            List<Long> chunk = addresses.subList(from, Math.min(from + TAKE_CHUNK, addresses.size()));
            StringBuilder sql = new StringBuilder(chunk.size() * 12 + 16).append("_rowaddr IN (");
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(chunk.get(i).longValue());
            }
            sql.append(')');
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(takeColumns)
                .filter(sql.toString())
                .withRowAddress(true)
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector[] vectors = new FieldVector[takeColumns.size()];
                    for (int c = 0; c < vectors.length; c++) {
                        vectors[c] = root.getVector(takeColumns.get(c));
                    }
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        Object[] row = new Object[vectors.length];
                        for (int c = 0; c < vectors.length; c++) {
                            row[c] = decodeTakeValue(takeColumns.get(c), vectors[c], i);
                        }
                        takenRows.put(offset, row);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        for (int docId : requested) {
            takenRows.putIfAbsent(docId, MISSING_ROW);
        }
    }

    /**
     * Decode one cell of a take-scan batch into the representation
     * {@link #materialiseStoredFields} renders from. Numeric columns
     * go through {@link #readAsLong} so the value carries the same
     * encoding as the doc value path (sortable-int / sortable-long for
     * floats, epoch millis for dates and timestamps, raw bit pattern
     * for UInt64); booleans become {@link Boolean}; Utf8 becomes
     * {@link String}; List&lt;Utf8&gt; becomes {@code String[]}; Binary
     * / LargeBinary become {@code byte[]}. A column that is not in
     * {@link #columnKind} (only the appended PK can be) is decoded by
     * vector type: Utf8 as a string, anything {@link #readAsLong}
     * understands as a long, otherwise {@code null}. Arrow nulls
     * return {@code null}.
     */
    private Object decodeTakeValue(String name, FieldVector vector, int i) {
        if (vector == null || vector.isNull(i)) {
            return null;
        }
        ColumnKind kind = columnKind.get(name);
        if (kind == null) {
            if (vector instanceof VarCharVector vc) {
                return new String(vc.get(i), java.nio.charset.StandardCharsets.UTF_8);
            }
            try {
                return readAsLong(vector, i);
            } catch (IllegalStateException unsupported) {
                return null;
            }
        }
        return switch (kind) {
            case NUMERIC -> readAsLong(vector, i);
            case BOOLEAN -> ((BitVector) vector).get(i) == 1;
            case TEXT_FTS, TEXT_KEYWORD -> new String(((VarCharVector) vector).get(i), java.nio.charset.StandardCharsets.UTF_8);
            case KEYWORD_ARRAY -> {
                ListVector list = (ListVector) vector;
                VarCharVector elements = (VarCharVector) list.getDataVector();
                int start = list.getElementStartIndex(i);
                int end = list.getElementEndIndex(i);
                String[] arr = new String[end - start];
                for (int e = start; e < end; e++) {
                    arr[e - start] = elements.isNull(e) ? null : new String(elements.get(e), java.nio.charset.StandardCharsets.UTF_8);
                }
                yield arr;
            }
            case BINARY -> {
                if (vector instanceof VarBinaryVector vb) {
                    yield vb.get(i);
                }
                if (vector instanceof LargeVarBinaryVector lb) {
                    yield lb.get(i);
                }
                yield null;
            }
        };
    }

    /**
     * Materialise stored fields for a single doc. Extracted from the {@code
     * storedFields()} anonymous class so the sequential wrapper (see
     * {@link LanceSequentialLeafReader}) can call the same routine when
     * {@code FetchPhase} switches to the sequential stored-fields path.
     *
     * <p>Reads from the row {@link #prefetchRows} fetched for
     * {@code docID}. Callers that know the whole page up front
     * (the fragment dispatch handler) prefetch every hit in one scan
     * per leaf; a doc that was not prefetched triggers a single-row
     * take here so the method stays correct for any caller.
     */
    public void materialiseStoredFields(int docID, StoredFieldVisitor visitor) throws IOException {
        FieldInfo idInfo = storedOnly("_id", 1);
        FieldInfo sourceInfo = storedOnly("_source", 2);
        boolean needsId = visitor.needsField(idInfo) == StoredFieldVisitor.Status.YES;
        boolean needsSource = visitor.needsField(sourceInfo) == StoredFieldVisitor.Status.YES;
        if (!needsId && !needsSource) {
            return;
        }
        Object[] row = takenRows.get(docID);
        if (row == null) {
            prefetchRows(new int[] { docID });
            row = takenRows.getOrDefault(docID, MISSING_ROW);
        }
        if (needsId) {
            // Materialise _id from whichever column the declared primary key
            // lives on, or synthesise "<fragment>-<offset>" when no PK is
            // declared. Without this fallback every row on a PK-less table
            // would collapse to the same _id, silently breaking sort-by-_id
            // and _mget dedup. GET /_doc/{id} still returns 404 for PK-less
            // tables (see LanceReadOnlyEngine.get); the fallback is strictly
            // for _search response fidelity.
            //
            // A null PK value (nullable column, Arrow null in that row) or
            // a row the take did not return also fall through to the
            // synthesised form so the row still gets a unique id rather
            // than repeating an empty string.
            Object pk = pkTakeIndex >= 0 && pkTakeIndex < row.length ? row[pkTakeIndex] : null;
            String idString = switch (pkType) {
                case KEYWORD -> pk instanceof String s ? s : fragmentId + "-" + docID;
                case LONG -> pk instanceof Long l ? Long.toString(l) : fragmentId + "-" + docID;
                // readAsLong returns the unsigned bit pattern for
                // UInt8Vector; Long.toUnsignedString decodes it back into
                // the 0..2^64-1 range the operator wrote.
                case UNSIGNED_LONG -> pk instanceof Long l ? Long.toUnsignedString(l) : fragmentId + "-" + docID;
                default -> fragmentId + "-" + docID;
            };
            org.apache.lucene.util.BytesRef encoded = org.opensearch.index.mapper.Uid.encodeId(idString);
            byte[] bytes = new byte[encoded.length];
            System.arraycopy(encoded.bytes, encoded.offset, bytes, 0, encoded.length);
            visitor.binaryField(idInfo, bytes);
        }
        if (needsSource) {
            // Build _source through XContentBuilder so string values get the
            // JSON escaping RFC 8259 requires (control characters U+0000
            // through U+001F, quotes, backslashes). Hand-rolled string
            // concatenation only escaped \" and \\, which meant a body
            // containing a newline emitted invalid JSON and every client
            // that parsed the response strictly (jackson, python json,
            // Dashboards) rejected it.
            //
            // Column iteration follows the schema pass order captured in
            // takeColumns (the leading sourceColumnCount entries mirror
            // columnKind), so _source keys land in the same order every
            // time. Values come from the per-hit take; no whole-column
            // load happens here.
            try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.startObject();
                int limit = Math.min(sourceColumnCount, row.length);
                for (int c = 0; c < limit; c++) {
                    Object value = row[c];
                    if (value == null) {
                        continue;
                    }
                    String name = takeColumns.get(c);
                    switch (columnKind.get(name)) {
                        case NUMERIC -> {
                            long numericValue = (Long) value;
                            NumericPrecision precision = numericPrecision.getOrDefault(name, NumericPrecision.INTEGER);
                            switch (precision) {
                                case FLOAT -> builder.field(
                                    name,
                                    org.apache.lucene.util.NumericUtils.sortableIntToFloat((int) numericValue)
                                );
                                case DOUBLE -> builder.field(name, org.apache.lucene.util.NumericUtils.sortableLongToDouble(numericValue));
                                case INTEGER -> {
                                    if (pkType == org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.UNSIGNED_LONG
                                        && name.equals(fieldName)) {
                                        // UInt64 PK column: emit as an unsigned
                                        // decimal so the JSON number matches
                                        // what the operator wrote. Other
                                        // UInt64 columns are not surfaced by
                                        // classify(), so this branch fires
                                        // only for the PK.
                                        builder.field(name, new java.math.BigInteger(Long.toUnsignedString(numericValue)));
                                    } else {
                                        builder.field(name, numericValue);
                                    }
                                }
                            }
                        }
                        case BOOLEAN -> builder.field(name, (Boolean) value);
                        case TEXT_FTS, TEXT_KEYWORD -> builder.field(name, (String) value);
                        case KEYWORD_ARRAY -> builder.field(name, (String[]) value);
                        case BINARY -> builder.field(name, java.util.Base64.getEncoder().encodeToString((byte[]) value));
                    }
                }
                builder.endObject();
                byte[] json = org.opensearch.core.common.bytes.BytesReference.toBytes(
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder)
                );
                visitor.binaryField(sourceInfo, json);
            }
        }
    }

    public Dataset dataset() {
        return dataset;
    }

    public int fragmentId() {
        return fragmentId;
    }

    /**
     * Recursively unwrap {@link FilterLeafReader} layers and return the
     * underlying {@code LanceFragmentLeafReader}, or {@code null} if the
     * leaf is not backed by Lance. Callers that need Lance-specific state
     * (fragment id, dataset handle) use this to strip any wrappers that
     * OpenSearch or Lucene may have applied to the reader.
     */
    public static LanceFragmentLeafReader unwrap(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader filter) {
            current = filter.getDelegate();
        }
        return current instanceof LanceFragmentLeafReader lance ? lance : null;
    }

    private static FieldInfo storedOnly(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
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

    // Reads an Arrow scalar vector value as a long. Callers guard against
    // nulls; this method assumes the input index has a value. Date/Timestamp
    // vectors are normalized to epoch milliseconds so the DateFieldMapper
    // reads them through the same numeric doc value path as integers.
    public static long readAsLong(FieldVector v, int i) {
        if (v instanceof org.apache.arrow.vector.TinyIntVector t) {
            return t.get(i);
        }
        if (v instanceof org.apache.arrow.vector.SmallIntVector s) {
            return s.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof org.apache.arrow.vector.UInt8Vector u) {
            // UInt64 comes back as UInt8Vector in Arrow Java. Its get()
            // returns a Java long that already carries the unsigned bit
            // pattern; the _id path decodes it via
            // Long.toUnsignedString and the doc value path exposes it
            // untouched so OpenSearch's unsigned_long field type
            // reinterprets the sign bit.
            return u.get(i);
        }
        if (v instanceof Float4Vector f) {
            // Store the sortable-int encoding widened to long. OpenSearch's
            // FloatFieldType decodes on the way out by casting the long back
            // to int and calling NumericUtils.sortableIntToFloat, so range
            // / sort / aggregation stay accurate. Explicit int → long
            // widening keeps the sign extension consistent with
            // NumericDocValues callers that read the raw long.
            return org.apache.lucene.util.NumericUtils.floatToSortableInt(f.get(i));
        }
        if (v instanceof Float8Vector f) {
            // Same shape as Float4Vector but the sortable representation
            // is already 64-bit so no widening is needed. DoubleFieldType
            // decodes with NumericUtils.sortableLongToDouble.
            return org.apache.lucene.util.NumericUtils.doubleToSortableLong(f.get(i));
        }
        if (v instanceof DateDayVector d) {
            return d.get(i) * 86_400_000L;
        }
        if (v instanceof DateMilliVector d) {
            return d.get(i);
        }
        if (v instanceof TimeStampSecVector t) {
            return t.get(i) * 1000L;
        }
        if (v instanceof TimeStampSecTZVector t) {
            return t.get(i) * 1000L;
        }
        if (v instanceof TimeStampMilliVector t) {
            return t.get(i);
        }
        if (v instanceof TimeStampMilliTZVector t) {
            return t.get(i);
        }
        if (v instanceof TimeStampMicroVector t) {
            return t.get(i) / 1000L;
        }
        if (v instanceof TimeStampMicroTZVector t) {
            return t.get(i) / 1000L;
        }
        if (v instanceof TimeStampNanoVector t) {
            return t.get(i) / 1_000_000L;
        }
        if (v instanceof TimeStampNanoTZVector t) {
            return t.get(i) / 1_000_000L;
        }
        throw new IllegalStateException("unsupported vector type: " + v.getClass().getName());
    }

    @Override
    public void checkIntegrity() {}

    @Override
    protected void doClose() throws IOException {
        cacheLifetimeBridge.close();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return cacheLifetimeBridge.leaves().get(0).reader().getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge.getReaderCacheHelper();
    }
}
