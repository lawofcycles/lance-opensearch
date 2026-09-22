/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.lucene.index.OpenSearchLeafReader;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.lance.engine.LanceFragmentSchema.NumericPrecision;

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
 *
 * <p>What the leaf knows about the table (column kinds, primary key,
 * field infos, take projection) lives in a {@link LanceFragmentSchema}
 * that is derived once per table version and shared; the leaf itself is
 * a light view that holds only request scoped state (the hint, the rows
 * taken for {@code _source}, sparse structures, columns loaded for this
 * request). A leaf built from a {@link LanceWarmCache} snapshot reads
 * numeric, boolean and keyword columns from the off-heap
 * {@link ColumnStore} through entries another request may have loaded
 * ({@link CachedColumn}, {@link CachedKeywordColumn},
 * {@link CachedKeywordArrayColumn}).
 */
public final class LanceFragmentLeafReader extends LeafReader {

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
    /**
     * Top-level Struct column names with at least one surfaced child.
     * The row take projects the whole struct under the parent name;
     * {@link #decodeTakeValue} turns it into a nested map so
     * {@link #materialiseStoredFields} renders a JSON object, while the
     * children's doc values live under dotted paths in
     * {@link #columnKind} and load through the same per-column scans as
     * top-level columns (Lance projects a dotted path as a flat column
     * aliased to it).
     */
    private final Set<String> structColumns;
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
    /** Shared, immutable description of the table's columns; see {@link LanceFragmentSchema}. */
    private final LanceFragmentSchema schema;
    // Column kind in schema order, from the shared schema. Preserves schema
    // order so materialiseStoredFields emits _source keys in schema order
    // regardless of which columns have been loaded so far.
    private final Map<String, ColumnKind> columnKind;
    /**
     * Precision override for numeric columns whose {@link ColumnKind} is
     * {@link ColumnKind#NUMERIC} but whose underlying Arrow type is not
     * a plain integer or a date/timestamp ({@code Float32} and
     * {@code Float64}); every other numeric column is absent from the
     * map and defaults to {@link NumericPrecision#INTEGER} inside the
     * {@code getOrDefault} lookups.
     */
    private final Map<String, NumericPrecision> numericPrecision;
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
    /**
     * Numeric and boolean columns of this fragment served from the
     * off-heap {@link ColumnStore}, published by
     * {@link LanceShardColumnCache} through {@link #publishOffHeapColumn}.
     * A column present here is complete for every physical row, so it
     * takes the place of the heap arrays above; the two never hold the
     * same column at once because the shard cache tries the store first
     * and only loads into heap when the store has no room.
     */
    private final Map<String, CachedColumn> offHeapColumns = new ConcurrentHashMap<>();
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
     * Keyword columns of this fragment served from the off-heap
     * {@link ColumnStore} (dictionary plus per-row ordinals), published
     * by {@link LanceShardColumnCache} through
     * {@link #publishOffHeapKeywordColumn}. Same exclusivity with the
     * heap maps above as {@link #offHeapColumns}: the shard cache tries
     * the store first and loads into heap only when the store has no
     * room or the reader carries a top-level filter.
     */
    private final Map<String, CachedKeywordColumn> offHeapKeywordColumns = new ConcurrentHashMap<>();
    /** Multi-valued counterpart of {@link #offHeapKeywordColumns}. */
    private final Map<String, CachedKeywordArrayColumn> offHeapKeywordArrayColumns = new ConcurrentHashMap<>();

    /**
     * Largest fraction of {@link #maxDoc} a hinted hit set may cover
     * before the doc value accessors stop taking the hit rows by
     * address and load the whole column instead.
     *
     * <p>A {@code _rowaddr IN (...)} take costs about 8 µs per row
     * (500,860 rows took 3.9 s on a 20M row table, one node), because
     * every row is a random read through the Lance take path. A full
     * column load is one sequential scan and costs about 20 ns per row
     * once the column is decoded (a 250,000 row fragment in a few
     * milliseconds; the seconds a first load of a 20M row column takes
     * are dominated by building the per request heap arrays, which the
     * off-heap {@link ColumnStore} pays once per table version). The
     * two meet at {@code 20 ns / 8 µs = 0.0025}, one row in four
     * hundred, so above that the sequential scan is used. The value is
     * a ratio of per-row costs and does not depend on the fragment's
     * row count.
     *
     * <p>Measured on the same table with a query matching 2.5 percent
     * of the rows ({@code sort ts desc}, 20M rows: 3.7 s with the
     * column scan against 4.13 s with the take; 100M rows, 2,502,753
     * hits: 19.2 s against 20.9 s), so a threshold above the ratio
     * makes such queries slower than the scan.
     *
     * <p>The ratio is only consulted when the column is not already at
     * hand: rows the store holds for the fragment are read from the
     * store whatever the hit set's size (see
     * {@link HintedNumericDocValues#resolve}). Kept as a single
     * constant so the threshold can be tuned in one place.
     */
    static final double SPARSE_RATIO = 0.0025;

    /**
     * Request-scoped hit set for this leaf, or {@code null} when no
     * Lance-side scorer has reported one. Sorted ascending doc ids
     * (physical row offsets) that the last {@link #hintMatchedOffsets}
     * call passed in. The doc value accessors use it to fetch only the
     * hit rows of a sort or aggregation column instead of scanning the
     * whole column; see {@link #hintMatchedOffsets} for the contract.
     *
     * <p>This state belongs to the request that is running against the
     * reader, not to the fragment. A reader that is later kept across
     * requests (a warm reader cache) must clear it, or receive a fresh
     * hint, before the next request reads doc values; today every
     * fragment path request opens its own reader, so the field lives
     * exactly as long as the request.
     */
    private volatile int[] hintedOffsets;
    /**
     * Whether the Lance-side scorer that delivered {@link #hintedOffsets}
     * has shown that every doc the current search collects on this
     * leaf is one of the hinted docs (its own {@code BulkScorer} drives
     * collection). Ordinal-based doc values ({@link #getSortedDocValues},
     * {@link #getSortedSetDocValues}) build their sparse term
     * dictionary only under this flag, because an ordinal space cannot
     * be widened after a consumer has observed it. Numeric doc values
     * use the hint without the flag and fall back to the full column
     * on the first doc outside the hint.
     */
    private volatile boolean hintExclusive;
    /**
     * Sparse numeric and boolean values taken for {@link #hintedOffsets},
     * keyed by column name. Cleared whenever the hint is replaced.
     */
    private final Map<String, SparseNumeric> sparseNumeric = new ConcurrentHashMap<>();
    /** Sparse keyword dictionaries taken for {@link #hintedOffsets}, keyed by base column name. */
    private final Map<String, SparseKeyword> sparseKeyword = new ConcurrentHashMap<>();
    /** Sparse multi-valued keyword dictionaries taken for {@link #hintedOffsets}, keyed by column name. */
    private final Map<String, SparseKeywordArray> sparseKeywordArray = new ConcurrentHashMap<>();
    /**
     * Per ordinal-based column, whether the first doc values instance
     * this leaf served under the current hint used the sparse
     * dictionary ({@code TRUE}) or the full one ({@code FALSE}). Every
     * later instance of the same column follows the first, so a global
     * ordinal map built from one instance and the instance a leaf
     * collector obtains afterwards agree on the ordinal space. A
     * {@code FALSE} entry survives hint replacement (the full
     * dictionary is loaded and stays valid); {@code TRUE} entries are
     * dropped together with the sparse dictionaries.
     */
    private final Map<String, Boolean> keywordServedSparse = new ConcurrentHashMap<>();

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
     * directly, callers that skip {@link LanceDirectoryReader}). The
     * single fragment heap loads route their request breaker charges
     * through the cache too, so one place gives them back on close.
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
    //
    // Built on the first getCoreCacheHelper / getReaderCacheHelper call rather
    // than in the constructor: the bridge costs an IndexWriter, a commit and a
    // DirectoryReader.open, and the fragment path opens one leaf per fragment
    // per request, so an aggregation over hundreds of fragments would pay that
    // per leaf although nothing on its path asks for a leaf-level cache key
    // (the query cache is disabled, the request cache keys off the composite
    // reader, and numeric doc values fielddata is built without the cache).
    // Consumers that do ask (the bitset filter cache for nested docs, global
    // ordinals fielddata, a DLS/FLS reader wrapper) get the same bridge for
    // the life of the leaf. Guarded by `this` and published through the
    // volatile field; helpers may be requested from any slice thread.
    // `closed` (also guarded by `this`) keeps a helper request that races
    // with close from building a bridge doClose has already read as
    // absent, which nothing would ever close.
    private volatile DirectoryReader cacheLifetimeBridge;
    private boolean closed;

    /**
     * Resolve which Utf8 columns of {@code dataset} carry an FTS
     * (inverted) index; see {@link LanceFragmentSchema#resolveFtsColumns}.
     */
    static java.util.Set<String> resolveFtsColumns(Dataset dataset) throws IOException {
        return LanceFragmentSchema.resolveFtsColumns(dataset);
    }

    /**
     * Open a leaf over one Lance fragment, deriving the schema and the
     * live-row bitmap for this leaf alone.
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
     * <p>Callers that open many leaves over one table derive the schema
     * once with {@link LanceFragmentSchema#derive} and use the
     * package-private constructor instead; the fragment path does so
     * through {@link LanceWarmCache}.
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
        this(
            dataset,
            fragmentId,
            physicalRows,
            hasDeletionFile,
            LanceFragmentSchema.derive(dataset, intField, pkType, multiFields, ftsColumns),
            filterSql
        );
    }

    /**
     * Open a leaf over one Lance fragment with a schema the caller
     * derived, resolving the live-row bitmap here (one {@code _rowaddr}
     * scan when the fragment has a deletion file).
     */
    LanceFragmentLeafReader(
        Dataset dataset,
        int fragmentId,
        long physicalRows,
        boolean hasDeletionFile,
        LanceFragmentSchema schema,
        String filterSql
    ) throws IOException {
        this(dataset, fragmentId, resolveFragmentMeta(dataset, fragmentId, physicalRows, hasDeletionFile), schema, filterSql);
    }

    private static LanceWarmCache.FragmentMeta resolveFragmentMeta(
        Dataset dataset,
        int fragmentId,
        long physicalRows,
        boolean hasDeletionFile
    ) throws IOException {
        LanceWarmCache.FragmentMeta meta = new LanceWarmCache.FragmentMeta(fragmentId, physicalRows, hasDeletionFile);
        meta.resolveLiveDocs(dataset);
        return meta;
    }

    /**
     * Open a leaf view over one fragment of a table whose schema and
     * fragment metadata are already known. Allocates only Lucene-side
     * objects: no Lance call happens here. {@code meta} must have its
     * live-row bitmap resolved ({@link LanceWarmCache.FragmentMeta#resolveLiveDocs}).
     *
     * @param dataset   shared dataset the leaf scans; not owned
     * @param fragmentId Lance fragment id this leaf exposes
     * @param meta      row count and live-row bitmap of the fragment
     * @param schema    column kinds, primary key and field infos
     * @param filterSql predicate for request scoped heap column loads, or {@code null}
     */
    LanceFragmentLeafReader(
        Dataset dataset,
        int fragmentId,
        LanceWarmCache.FragmentMeta meta,
        LanceFragmentSchema schema,
        String filterSql
    ) {
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.schema = schema;
        this.fieldName = schema.fieldName();
        this.pkType = schema.pkType();
        this.maxDoc = meta.physicalRows();
        this.filterSql = filterSql;
        this.keywordSubFields = schema.keywordSubFields();
        this.basesWithKeywordSub = schema.basesWithKeywordSub();
        this.structColumns = schema.structColumns();
        this.columnKind = schema.columnKind();
        this.numericPrecision = schema.numericPrecision();
        this.sourceColumnCount = schema.sourceColumnCount();
        this.pkTakeIndex = schema.pkTakeIndex();
        this.takeColumns = schema.takeColumns();
        this.numDocs = meta.numDocs();
        this.liveDocs = meta.liveDocs();
        this.fieldInfos = schema.fieldInfos();
    }

    /**
     * The one-doc Lucene reader backing {@link #getCoreCacheHelper()}
     * and {@link #getReaderCacheHelper()}, built on first use (see the
     * field comment for why it is not built in the constructor). The
     * instance is stable for the life of this leaf, as the cache
     * helper contract requires.
     *
     * @throws AlreadyClosedException when the
     *         leaf was closed before any helper was requested; building
     *         a bridge then would leak it, because {@link #doClose()}
     *         has already read the field as absent
     */
    private DirectoryReader cacheLifetimeBridge() {
        DirectoryReader bridge = cacheLifetimeBridge;
        if (bridge != null) {
            return bridge;
        }
        synchronized (this) {
            bridge = cacheLifetimeBridge;
            if (bridge != null) {
                return bridge;
            }
            if (closed) {
                throw new AlreadyClosedException("this LanceFragmentLeafReader was closed before a cache helper was requested");
            }
            try {
                ByteBuffersDirectory bridgeDir = new ByteBuffersDirectory();
                try (IndexWriter writer = new IndexWriter(bridgeDir, new IndexWriterConfig())) {
                    writer.addDocument(new Document());
                    writer.commit();
                }
                bridge = DirectoryReader.open(bridgeDir);
            } catch (IOException e) {
                // getCoreCacheHelper / getReaderCacheHelper cannot
                // throw a checked exception; an in-heap one-doc index
                // only fails when the JVM is already in trouble.
                throw new UncheckedIOException("could not build the cache lifetime bridge", e);
            }
            cacheLifetimeBridge = bridge;
            return bridge;
        }
    }

    /** Whether the cache lifetime bridge has been built. Test observability only. */
    boolean cacheLifetimeBridgeExists() {
        return cacheLifetimeBridge != null;
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
        ensureNumericLoaded(name, true);
    }

    /**
     * Same as {@link #ensureNumericLoaded(String)}; {@code useShardCache}
     * false keeps the load to this fragment even when a
     * {@link LanceShardColumnCache} is installed. A doc values instance
     * that leaves the sparse path for one doc outside its hint uses this
     * so the fallback costs one fragment, not one scan of every fragment
     * in the reader; the other leaves keep serving their hinted rows. The
     * fragment is still served from the off-heap store when the shard
     * cache has one, and scanned into heap otherwise.
     */
    private void ensureNumericLoaded(String name, boolean useShardCache) throws IOException {
        if (numericColumns.containsKey(name) || offHeapColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null && useShardCache) {
            // Delegate to the shard-level coordinator: one scan for
            // the whole reader instead of one per leaf. After the
            // cache returns, publishNumericColumn or
            // publishOffHeapColumn below has put the fragment's slice
            // into this leaf's maps, so the short-circuit above fires
            // on subsequent calls.
            cache.loadNumericColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (numericColumns.containsKey(name) || offHeapColumns.containsKey(name)) {
                return;
            }
            if (cache != null && cache.publishFromStoreForLeaf(this, name, false)) {
                return;
            }
            long heapBytes = LanceShardColumnCache.numericHeapBytes(maxDoc);
            chargeHeap(cache, heapBytes, name);
            boolean published = false;
            try {
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
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(cache, heapBytes);
                }
            }
        }
    }

    /**
     * Charge {@code bytes} of heap this leaf is about to allocate for a
     * single fragment load of {@code name} to the request breaker, through
     * the shard cache so the charge is given back when the reader closes.
     * A leaf without a cache (tests that build leaves directly) has no
     * breaker and allocates unchecked.
     */
    private static void chargeHeap(LanceShardColumnCache cache, long bytes, String name) {
        if (cache != null) {
            cache.chargeHeap(bytes, name);
        }
    }

    /** Give back a charge of {@link #chargeHeap} whose load did not complete. */
    private static void releaseHeap(LanceShardColumnCache cache, long bytes) {
        if (cache != null) {
            cache.releaseHeap(bytes);
        }
    }

    /**
     * The request breaker heap column loads of this leaf are charged to:
     * the shard cache's when one is attached, a {@link NoopCircuitBreaker}
     * otherwise.
     */
    CircuitBreaker requestBreaker() {
        LanceShardColumnCache cache = shardColumnCache;
        return cache == null ? new NoopCircuitBreaker(CircuitBreaker.REQUEST) : cache.requestBreaker();
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
     * Called by {@link LanceShardColumnCache} when the off-heap
     * {@link ColumnStore} holds (or has just loaded) the numeric or
     * boolean column {@code name} for this fragment. The column is
     * pinned by the shard cache for the life of the request, so the
     * doc values instances created afterwards read it directly.
     */
    void publishOffHeapColumn(String name, CachedColumn column) {
        offHeapColumns.put(name, column);
    }

    /** Off-heap column of {@code name} published for this leaf, or {@code null}. */
    CachedColumn offHeapColumn(String name) {
        return offHeapColumns.get(name);
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

    /** The {@link LanceShardColumnCache} attached to this leaf, or {@code null}; for tests that read its counters. */
    LanceShardColumnCache shardColumnCache() {
        return shardColumnCache;
    }

    private void ensureBooleanLoaded(String name) throws IOException {
        ensureBooleanLoaded(name, true);
    }

    private void ensureBooleanLoaded(String name, boolean useShardCache) throws IOException {
        if (booleanColumns.containsKey(name) || offHeapColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null && useShardCache) {
            cache.loadBooleanColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (booleanColumns.containsKey(name) || offHeapColumns.containsKey(name)) {
                return;
            }
            if (cache != null && cache.publishFromStoreForLeaf(this, name, true)) {
                return;
            }
            long heapBytes = LanceShardColumnCache.numericHeapBytes(maxDoc);
            chargeHeap(cache, heapBytes, name);
            boolean published = false;
            try {
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
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(cache, heapBytes);
                }
            }
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
     * is installed so the scan runs once per shard, and so the column is
     * served from the off-heap store when the reader has one.
     */
    private void ensureTextLoaded(String name) throws IOException {
        ensureTextLoaded(name, true);
    }

    private void ensureTextLoaded(String name, boolean useShardCache) throws IOException {
        if (keywordOrds.containsKey(name) || offHeapKeywordColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null && useShardCache) {
            cache.loadTextColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (keywordOrds.containsKey(name) || offHeapKeywordColumns.containsKey(name)) {
                return;
            }
            if (cache != null && cache.publishKeywordFromStoreForLeaf(this, name)) {
                return;
            }
            long charged = LanceShardColumnCache.intArrayBytes(maxDoc);
            chargeHeap(cache, charged, name);
            boolean published = false;
            try {
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
                long termBytes = LanceShardColumnCache.termsHeapBytes(builder.size(), builder.termBytes());
                chargeHeap(cache, termBytes, name);
                charged += termBytes;
                KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
                dictionary.remap(ids);
                publishTextColumn(name, dictionary.terms(), ids);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(cache, charged);
                }
            }
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
     * Called by {@link LanceShardColumnCache} when the off-heap
     * {@link ColumnStore} holds (or has just loaded) the dictionary and
     * ordinals of the Utf8 column {@code name} for this fragment. Pinned
     * by the shard cache for the life of the request.
     */
    void publishOffHeapKeywordColumn(String name, CachedKeywordColumn column) {
        offHeapKeywordColumns.put(name, column);
    }

    /** Multi-valued counterpart of {@link #publishOffHeapKeywordColumn}. */
    void publishOffHeapKeywordArrayColumn(String name, CachedKeywordArrayColumn column) {
        offHeapKeywordArrayColumns.put(name, column);
    }

    /** Off-heap keyword column of {@code name} published for this leaf, or {@code null}. */
    CachedKeywordColumn offHeapKeywordColumn(String name) {
        return offHeapKeywordColumns.get(name);
    }

    /** Off-heap keyword array column of {@code name} published for this leaf, or {@code null}. */
    CachedKeywordArrayColumn offHeapKeywordArrayColumn(String name) {
        return offHeapKeywordArrayColumns.get(name);
    }

    /**
     * Build the multi-valued keyword dictionary for a List&lt;Utf8&gt;
     * column: sorted terms plus, per doc, a strictly ascending
     * duplicate-free ordinal array (null for an Arrow-null list), for
     * {@link #getSortedSetDocValues}.
     */
    private void ensureKeywordArrayLoaded(String name) throws IOException {
        ensureKeywordArrayLoaded(name, true);
    }

    private void ensureKeywordArrayLoaded(String name, boolean useShardCache) throws IOException {
        if (keywordArrayOrds.containsKey(name) || offHeapKeywordArrayColumns.containsKey(name)) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache != null && useShardCache) {
            cache.loadKeywordArrayColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (keywordArrayOrds.containsKey(name) || offHeapKeywordArrayColumns.containsKey(name)) {
                return;
            }
            if (cache != null && cache.publishKeywordArrayFromStoreForLeaf(this, name)) {
                return;
            }
            long charged = LanceShardColumnCache.objectArrayBytes(maxDoc);
            chargeHeap(cache, charged, name);
            boolean published = false;
            try {
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
                int[] idToOrd = builder.sort();
                for (int r = 0; r < rows.length; r++) {
                    if (rows[r] != null) {
                        rows[r] = KeywordDictionaryBuilder.remapSortedUnique(idToOrd, rows[r]);
                    }
                }
                long rowAndTermBytes = LanceShardColumnCache.rowOrdinalBytes(rows) + LanceShardColumnCache.termsHeapBytes(
                    builder.size(),
                    builder.termBytes()
                );
                chargeHeap(cache, rowAndTermBytes, name);
                charged += rowAndTermBytes;
                publishKeywordArrayColumn(name, builder.finish().terms(), rows);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(cache, charged);
                }
            }
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

    /**
     * Report the doc ids a Lance-side scorer ({@code LanceFtsQuery},
     * {@code LanceKnnQuery}) matched on this leaf, so the doc value
     * accessors can fetch a sort or aggregation column for those rows
     * only instead of scanning the whole column. Lucene's sort
     * comparators and aggregators read doc values for the collected
     * docs alone, so when the collected docs are the scorer's hits a
     * {@code _rowaddr IN (...)} take of the hit rows is all the column
     * data the request needs.
     *
     * <p>{@code sortedOffsets} must be sorted ascending and free of
     * duplicates; the array is kept by reference and must not be
     * modified afterwards. A second call replaces the previous hint
     * (no union): each Lucene {@code Weight} reports its own hit set,
     * and a later Weight of the same request (the count phase, or a
     * second Lance clause of a bool query) describes the docs the
     * collector is about to see better than the union would. When the
     * new array equals the current one the sparse structures already
     * taken are kept; otherwise they are dropped. An empty array never
     * displaces a non-empty hint (see the body for why that is safe).
     *
     * <p>{@code exclusive} states that the caller has established that
     * every doc the current search collects on this leaf is one of
     * {@code sortedOffsets}. The Lance scorers pass {@code false} when
     * they build their per-leaf scorer and {@code true} once Lucene
     * asks their {@code ScorerSupplier} for a {@code BulkScorer}, which
     * only the collection driver of a leaf (the searcher itself, or a
     * boolean parent whose other clauses can only narrow the doc set)
     * does. The fragment executor passes {@code true} ahead of
     * collection, through {@code LanceHintingWeight.hintExclusive},
     * when it knows from the request shape that the Lance clause is the
     * top-level query, so that aggregators and sort comparators built
     * afterwards already see the hint. Only ordinal-based doc values
     * depend on the flag; numeric doc values verify the hint per doc
     * and fall back to the full column when a doc outside it is
     * requested.
     *
     * <p>The hint is request-scoped state on a reader that today lives
     * for one request. A reader kept across requests must not carry a
     * hint from one request into the next.
     */
    public void hintMatchedOffsets(int[] sortedOffsets, boolean exclusive) {
        int[] current = hintedOffsets;
        if (current != null && (current == sortedOffsets || Arrays.equals(current, sortedOffsets))) {
            if (exclusive) {
                hintExclusive = true;
            }
            return;
        }
        if (sortedOffsets.length == 0 && current != null && current.length > 0) {
            // A scorer that matched nothing on this leaf does not
            // displace the rows another scorer of the same request
            // reported: whatever gets collected here is still inside
            // the standing hint (a disjunction collects the other
            // scorer's rows, a conjunction collects none), and an
            // exclusive empty set only says that nothing is collected,
            // which the standing hint covers as well.
            if (exclusive) {
                hintExclusive = true;
            }
            return;
        }
        hintedOffsets = sortedOffsets;
        hintExclusive = exclusive;
        sparseNumeric.clear();
        sparseKeyword.clear();
        sparseKeywordArray.clear();
        keywordServedSparse.values().removeIf(Boolean::booleanValue);
    }

    /** Current hint, for tests. */
    int[] hintedOffsets() {
        return hintedOffsets;
    }

    /** Whether the current hint is marked exclusive, for tests. */
    boolean hintExclusive() {
        return hintExclusive;
    }

    /**
     * Whether doc values of column {@code name} are currently served
     * from the rows taken for the hint, for tests. Numeric columns: a
     * take exists for the current hint and no instance has left it for
     * the full column. Keyword columns: the first instance under the
     * current hint chose the sparse dictionary.
     */
    boolean isServingSparse(String name) {
        SparseNumeric taken = sparseNumeric.get(name);
        if (taken != null) {
            return taken.offsets == hintedOffsets && !taken.fellBack;
        }
        return Boolean.TRUE.equals(keywordServedSparse.get(name));
    }

    /**
     * Whether the whole column {@code name} has been materialised on
     * this leaf (through {@code ensureXxxLoaded} or a
     * {@link LanceShardColumnCache} publish), for tests that check
     * which path a doc values accessor took.
     */
    boolean isColumnFullyLoaded(String name) {
        return numericColumns.containsKey(name)
            || booleanColumns.containsKey(name)
            || offHeapColumns.containsKey(name)
            || keywordOrds.containsKey(name)
            || keywordArrayOrds.containsKey(name)
            || offHeapKeywordColumns.containsKey(name)
            || offHeapKeywordArrayColumns.containsKey(name);
    }

    /** Whether column {@code name} is served from the off-heap column store on this leaf, for tests. */
    boolean isServingOffHeap(String name) {
        return offHeapColumns.containsKey(name) || offHeapKeywordColumns.containsKey(name) || offHeapKeywordArrayColumns.containsKey(name);
    }

    /**
     * Whether {@code hint} is small enough, relative to this leaf's
     * row count, for the per-row take to be cheaper than the full
     * column scan (see {@link #SPARSE_RATIO}).
     */
    private boolean isSparseHint(int[] hint) {
        return hint != null && hint.length <= maxDoc * SPARSE_RATIO;
    }

    /**
     * Serve the numeric or boolean column {@code name} to this leaf from
     * the off-heap {@link ColumnStore} when the store already holds this
     * fragment's slice, and report whether it did. Consulted before a
     * hinted take: a held column is read at the cost of one pin, so the
     * per-row take is never cheaper than it, whatever the hit set's
     * size. Only this fragment is pinned; the store is not asked to load
     * anything for fragments it does not hold, so a small hit set never
     * starts a store load as a side effect (the leaf falls through to
     * the take instead). Should the entry be evicted between the lookup
     * and the pin, the shard cache reloads this one fragment, which is
     * the same single fragment scan the leaf would run when a doc
     * outside the hint is requested.
     */
    private boolean serveHeldFromStore(String name, boolean isBoolean) throws IOException {
        LanceShardColumnCache cache = shardColumnCache;
        // No filterSql gate here, unlike the keyword lookup: a numeric
        // store entry holds one value per physical row, so a request
        // that carries a top-level filter still reads exactly its own
        // docs from it. A keyword dictionary built under a filter would
        // have a different ordinal space, which is why
        // storeHoldsKeyword declines when a filter is present.
        return cache != null && cache.storeHoldsColumn(this, name) && cache.publishFromStoreForLeaf(this, name, isBoolean);
    }

    /**
     * Whether the off-heap store holds the dictionary and ordinals of
     * the Utf8 or {@code List<Utf8>} column {@code name} for this
     * fragment, in a form this reader may read (no top-level filter).
     * {@link #serveKeywordSparse} declines the sparse dictionary when it
     * does, since the held dictionary is complete and costs no scan.
     */
    private boolean storeHoldsKeyword(String name) {
        LanceShardColumnCache cache = shardColumnCache;
        return cache != null && cache.storeHoldsKeyword(this, name);
    }

    /**
     * Keyword counterpart of {@link #serveHeldFromStore}, called by the
     * ordinal-based doc values once {@link #serveKeywordSparse} has
     * declined the sparse dictionary for an exclusive hint below
     * {@link #SPARSE_RATIO}. In that case the store holds this fragment's
     * dictionary, and this publishes it to this leaf alone, so the
     * {@code ensureXxxLoaded} call that follows finds the column present
     * instead of loading it for every leaf of the reader. A no-op when
     * the decision was not driven by the store (no exclusive sparse
     * hint, or a full dictionary already on the leaf). Should the entry
     * be evicted between the lookup and the pin, the shard cache
     * reloads this one fragment, the same single fragment scan the leaf
     * runs when a doc outside the hint is requested.
     */
    private void serveHeldKeywordInsteadOfTake(String name, int[] hint, boolean exclusive, boolean multiValued) throws IOException {
        if (!exclusive || !isSparseHint(hint)) {
            return;
        }
        boolean present = multiValued
            ? keywordArrayOrds.containsKey(name) || offHeapKeywordArrayColumns.containsKey(name)
            : keywordOrds.containsKey(name) || offHeapKeywordColumns.containsKey(name);
        if (present) {
            return;
        }
        LanceShardColumnCache cache = shardColumnCache;
        if (cache == null || !cache.storeHoldsKeyword(this, name)) {
            return;
        }
        if (multiValued) {
            cache.publishKeywordArrayFromStoreForLeaf(this, name);
        } else {
            cache.publishKeywordFromStoreForLeaf(this, name);
        }
    }

    /**
     * Values of one numeric or boolean column for the hinted rows.
     * {@code offsets} is the hint array itself; {@code values[i]} and
     * {@code presence.get(i)} describe the row at {@code offsets[i]}.
     * Rows the take did not return (deleted after the hit was
     * produced) keep their presence bit clear, which is also what the
     * full column load reports for them.
     */
    private static final class SparseNumeric {
        final int[] offsets;
        final long[] values;
        final FixedBitSet presence;
        /**
         * Set once a doc outside {@code offsets} was requested and the
         * accessor switched to the full column. Later instances for
         * the same column start on the full column directly instead
         * of repeating the miss.
         */
        volatile boolean fellBack;

        SparseNumeric(int[] offsets) {
            this.offsets = offsets;
            this.values = new long[offsets.length];
            this.presence = new FixedBitSet(offsets.length);
        }
    }

    /**
     * Keyword dictionary built from the hinted rows only: {@code terms}
     * sorted in unsigned byte order over the distinct values of those
     * rows, {@code ords[i]} the ordinal of the row at {@code offsets[i]}
     * or -1 for Arrow null. The ordinal space is that of the hinted
     * rows, which is complete for a consumer that only reads hinted
     * docs.
     */
    private static final class SparseKeyword {
        final int[] offsets;
        final int[] ords;
        final BytesRef[] terms;

        SparseKeyword(int[] offsets, int[] ords, BytesRef[] terms) {
            this.offsets = offsets;
            this.ords = ords;
            this.terms = terms;
        }
    }

    /** Multi-valued counterpart of {@link SparseKeyword}; {@code rowOrds[i]} is null for an Arrow-null list. */
    private static final class SparseKeywordArray {
        final int[] offsets;
        final int[][] rowOrds;
        final BytesRef[] terms;

        SparseKeywordArray(int[] offsets, int[][] rowOrds, BytesRef[] terms) {
            this.offsets = offsets;
            this.rowOrds = rowOrds;
            this.terms = terms;
        }
    }

    /** Receives one taken cell: the index into the hint array, the column vector and the row inside it. */
    @FunctionalInterface
    private interface TakenCellConsumer {
        void accept(int hintIndex, FieldVector vector, int row);
    }

    /**
     * Take the rows at {@code sortedOffsets} for the single column
     * {@code column} and hand every returned cell to {@code consumer}.
     * Same {@code _rowaddr IN (...)} shape and {@link #TAKE_CHUNK}
     * chunking as {@link #prefetchRows}: Lance turns the predicate into
     * a take by address, so the cost is proportional to the number of
     * offsets, not to the fragment's row count. {@link #filterSql} is
     * not layered in for the same reason as in {@code prefetchRows}:
     * the offsets come from a scan that already applied whatever
     * predicate the query carries.
     */
    private void takeHintedRows(String column, int[] sortedOffsets, TakenCellConsumer consumer) throws IOException {
        for (int from = 0; from < sortedOffsets.length; from += TAKE_CHUNK) {
            int to = Math.min(from + TAKE_CHUNK, sortedOffsets.length);
            StringBuilder sql = new StringBuilder((to - from) * 12 + 16).append("_rowaddr IN (");
            for (int i = from; i < to; i++) {
                if (i > from) {
                    sql.append(',');
                }
                sql.append(((long) fragmentId << 32) | (sortedOffsets[i] & 0xFFFFFFFFL));
            }
            sql.append(')');
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(Collections.singletonList(column))
                .filter(sql.toString())
                .withRowAddress(true)
                .build();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector vector = root.getVector(column);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        int hintIndex = Arrays.binarySearch(sortedOffsets, offset);
                        if (hintIndex >= 0) {
                            consumer.accept(hintIndex, vector, i);
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Sparse values of the numeric or boolean column {@code name} for
     * {@code hint}, taking them on first use. The same instance is
     * returned for every accessor of the column while the hint stands.
     */
    private SparseNumeric sparseNumericFor(String name, boolean isBoolean, int[] hint) throws IOException {
        SparseNumeric existing = sparseNumeric.get(name);
        if (existing != null && existing.offsets == hint) {
            return existing;
        }
        synchronized (columnLock(name)) {
            existing = sparseNumeric.get(name);
            if (existing != null && existing.offsets == hint) {
                return existing;
            }
            SparseNumeric fresh = new SparseNumeric(hint);
            if (isBoolean) {
                takeHintedRows(name, hint, (hintIndex, vector, row) -> {
                    if (!vector.isNull(row)) {
                        fresh.values[hintIndex] = ((BitVector) vector).get(row);
                        fresh.presence.set(hintIndex);
                    }
                });
            } else {
                takeHintedRows(name, hint, (hintIndex, vector, row) -> {
                    if (!vector.isNull(row)) {
                        fresh.values[hintIndex] = readAsLong(vector, row);
                        fresh.presence.set(hintIndex);
                    }
                });
            }
            sparseNumeric.put(name, fresh);
            return fresh;
        }
    }

    /** Sparse keyword dictionary of the Utf8 column {@code name} for {@code hint}, built on first use. */
    private SparseKeyword sparseKeywordFor(String name, int[] hint) throws IOException {
        SparseKeyword existing = sparseKeyword.get(name);
        if (existing != null && existing.offsets == hint) {
            return existing;
        }
        synchronized (columnLock(name)) {
            existing = sparseKeyword.get(name);
            if (existing != null && existing.offsets == hint) {
                return existing;
            }
            int[] ids = new int[hint.length];
            Arrays.fill(ids, -1);
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder();
            takeHintedRows(name, hint, (hintIndex, vector, row) -> {
                if (!vector.isNull(row)) {
                    ids[hintIndex] = builder.intern((VarCharVector) vector, row);
                }
            });
            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            dictionary.remap(ids);
            SparseKeyword fresh = new SparseKeyword(hint, ids, dictionary.terms());
            sparseKeyword.put(name, fresh);
            return fresh;
        }
    }

    /** Sparse multi-valued keyword dictionary of the List&lt;Utf8&gt; column {@code name} for {@code hint}, built on first use. */
    private SparseKeywordArray sparseKeywordArrayFor(String name, int[] hint) throws IOException {
        SparseKeywordArray existing = sparseKeywordArray.get(name);
        if (existing != null && existing.offsets == hint) {
            return existing;
        }
        synchronized (columnLock(name)) {
            existing = sparseKeywordArray.get(name);
            if (existing != null && existing.offsets == hint) {
                return existing;
            }
            int[][] rows = new int[hint.length][];
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder();
            takeHintedRows(name, hint, (hintIndex, vector, row) -> {
                if (!vector.isNull(row)) {
                    ListVector list = (ListVector) vector;
                    rows[hintIndex] = internListElements(builder, list, (VarCharVector) list.getDataVector(), row);
                }
            });
            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            for (int r = 0; r < rows.length; r++) {
                if (rows[r] != null) {
                    rows[r] = dictionary.remapSortedUnique(rows[r]);
                }
            }
            SparseKeywordArray fresh = new SparseKeywordArray(hint, rows, dictionary.terms());
            sparseKeywordArray.put(name, fresh);
            return fresh;
        }
    }

    /**
     * Decide, once per column and hint, whether the ordinal-based doc
     * values of {@code name} are served from the sparse dictionary.
     * The first decision sticks (see {@link #keywordServedSparse}).
     * Sparse requires an exclusive hint below {@link #SPARSE_RATIO},
     * no full dictionary already present on the leaf, and no dictionary
     * of this fragment in the off-heap store: a dictionary published by
     * the shard cache on behalf of another leaf, or held by the store
     * from an earlier request, is complete and free to read, so the take
     * cannot beat it. The exclusivity condition is what keeps the
     * ordinal space stable; the other two only choose the cheaper
     * complete source.
     */
    private boolean serveKeywordSparse(String name, int[] hint, boolean exclusive) {
        Boolean served = keywordServedSparse.get(name);
        if (served != null) {
            return served;
        }
        boolean sparse = exclusive
            && isSparseHint(hint)
            && !keywordOrds.containsKey(name)
            && !keywordArrayOrds.containsKey(name)
            && !offHeapKeywordColumns.containsKey(name)
            && !offHeapKeywordArrayColumns.containsKey(name)
            && !storeHoldsKeyword(name);
        Boolean previous = keywordServedSparse.putIfAbsent(name, sparse);
        return previous != null ? previous : sparse;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        ColumnKind kind = columnKind.get(field);
        if (kind != ColumnKind.NUMERIC && kind != ColumnKind.BOOLEAN) {
            return null;
        }
        return new HintedNumericDocValues(field, kind == ColumnKind.BOOLEAN);
    }

    /**
     * Numeric doc values that pick their data source on first use
     * rather than when the instance is created. Lucene's
     * {@code IndexSearcher.searchLeaf} obtains the leaf collector, and
     * with it the sort comparators' doc values, before it asks the
     * Weight for a scorer, so a Lance scorer's hint for the leaf
     * arrives after this instance exists but before the first
     * {@link #advanceExact}. Deferring the choice to that call lets
     * the hint be used.
     *
     * <p>On first use the data source is chosen in this order:
     * <ol>
     *   <li>rows already taken for the current hint on this leaf (a
     *       previous instance of the column took them, and none has
     *       since left them for the full column);</li>
     *   <li>the full column, when it is already present on this leaf
     *       (loaded by an earlier accessor, or published by the shard
     *       cache on behalf of another leaf);</li>
     *   <li>with a hint below {@link #SPARSE_RATIO}: the off-heap
     *       {@link ColumnStore} when it holds this fragment's column
     *       from an earlier request, read for this fragment alone and
     *       without loading anything else; otherwise a take of the
     *       hinted rows;</li>
     *   <li>otherwise the full column, loaded through the shard cache
     *       (into the store when it has room, into heap when not).</li>
     * </ol>
     * A sparse instance that is asked about a doc outside
     * the hint loads the full column at that moment and answers from
     * it for the rest of its life, so a Lucene clause that collects
     * docs the Lance scorer did not produce still sees correct values.
     * The switch is recorded on the sparse structure so later
     * instances of the same column start on the full column.
     *
     * <p>{@link #advance} and {@link #nextDoc} walk every doc of the
     * leaf, which a sparse structure cannot answer; they switch to the
     * full column as well.
     */
    private final class HintedNumericDocValues extends NumericDocValues {
        private final String name;
        private final boolean isBoolean;
        private boolean resolved;
        private long[] column;
        private FixedBitSet presence;
        private CachedColumn offHeap;
        private SparseNumeric sparse;
        private int sparseIndex = -1;
        private int doc = -1;

        HintedNumericDocValues(String name, boolean isBoolean) {
            this.name = name;
            this.isBoolean = isBoolean;
        }

        private void resolve() throws IOException {
            if (resolved) {
                return;
            }
            resolved = true;
            int[] hint = hintedOffsets;
            // Rows already taken for this hint serve as well as the full
            // column would, so they win even when a shard cache load on
            // behalf of another leaf has published the full column here.
            SparseNumeric taken = sparseNumeric.get(name);
            if (taken != null && taken.offsets == hint && !taken.fellBack) {
                sparse = taken;
                return;
            }
            if (hasFullColumn()) {
                useFullColumn();
                return;
            }
            if (isSparseHint(hint)) {
                // A slice the store already holds for this fragment is
                // read for one pin; the take is only cheaper than a load
                // the store has not done yet.
                if (serveHeldFromStore(name, isBoolean)) {
                    useFullColumn();
                    return;
                }
                SparseNumeric candidate = sparseNumericFor(name, isBoolean, hint);
                if (!candidate.fellBack) {
                    sparse = candidate;
                    return;
                }
            }
            // No usable hint: the whole column is needed. The shard cache
            // serves it from the off-heap column store when it holds or
            // can load the column, and loads it into heap for every leaf
            // of the reader in one scan otherwise.
            loadFullColumn(true);
        }

        private boolean hasFullColumn() {
            return offHeapColumns.containsKey(name) || (isBoolean ? booleanColumns : numericColumns).containsKey(name);
        }

        private void loadFullColumn(boolean useShardCache) throws IOException {
            if (isBoolean) {
                ensureBooleanLoaded(name, useShardCache);
            } else {
                ensureNumericLoaded(name, useShardCache);
            }
            useFullColumn();
        }

        private void useFullColumn() {
            offHeap = offHeapColumns.get(name);
            if (offHeap == null) {
                column = isBoolean ? booleanColumns.get(name) : numericColumns.get(name);
                presence = isBoolean ? booleanPresence.get(name) : numericPresence.get(name);
            }
            if (sparse != null) {
                sparse.fellBack = true;
                sparse = null;
            }
        }

        @Override
        public long longValue() {
            if (sparse != null) {
                return sparse.values[sparseIndex];
            }
            return offHeap != null ? offHeap.get(doc) : column[doc];
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index >= 0) {
                    sparseIndex = index;
                    return sparse.presence.get(index);
                }
                // A doc the Lance scorer did not produce: another
                // clause of the query drives collection on this leaf,
                // so the hint does not cover what will be asked. Read
                // the whole column from here on, scanning this fragment
                // only; the other leaves may still be served sparsely.
                loadFullColumn(false);
            }
            // presence bit is clear for Arrow-null docs; exists / term /
            // range / agg / sort all check advanceExact and stop reading
            // the value here.
            return offHeap != null ? offHeap.isSet(target) : presence.get(target);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            resolve();
            if (sparse != null) {
                loadFullColumn(false);
            }
            // Skip liveDocs holes and Arrow-null slots. DocValuesFieldExistsQuery
            // iterates through the doc values with advance/nextDoc alone and does
            // not call advanceExact, so the null bitmap must also be honoured here
            // - otherwise exists / _field_names checks count every row regardless
            // of presence.
            for (int candidate = target; candidate < maxDoc; candidate++) {
                if (liveDocs != null && !liveDocs.get(candidate)) {
                    continue;
                }
                if (offHeap != null ? offHeap.isSet(candidate) : presence.get(candidate)) {
                    doc = candidate;
                    return doc;
                }
            }
            doc = NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            // An estimate only; do not resolve here, because a caller
            // that asks for the cost while building the query tree
            // would fix the data source before the hint has arrived.
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
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
        return new HintedSortedDocValues(source);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        ColumnKind kind = columnKind.get(field);
        if (kind == ColumnKind.KEYWORD_ARRAY) {
            return new HintedSortedSetDocValues(field);
        }
        SortedDocValues single = getSortedDocValues(field);
        return single == null ? null : DocValues.singleton(single);
    }

    /**
     * Keyword doc values over a Utf8 column that choose between the
     * sparse dictionary of the hinted rows and the full dictionary on
     * first use, for the same reason {@link HintedNumericDocValues}
     * defers its choice.
     *
     * <p>Unlike numeric values, an ordinal space cannot change after a
     * consumer has seen it: a sort comparator keeps the ordinal of its
     * current bottom slot, and a global ordinal map records every
     * segment ordinal it saw when it was built. The source is chosen on
     * first use in this order, and the decision for the column is
     * recorded in {@link #keywordServedSparse} so every later instance
     * under the same hint uses the same dictionary:
     * <ol>
     *   <li>the decision already recorded for the column under the
     *       current hint;</li>
     *   <li>the full dictionary, when it is already present on this
     *       leaf;</li>
     *   <li>with an exclusive hint (every doc the search collects on
     *       this leaf is a hinted doc) below {@link #SPARSE_RATIO}: the
     *       off-heap {@link ColumnStore} when it holds this fragment's
     *       dictionary from an earlier request, read for this fragment
     *       alone; otherwise the sparse dictionary built from the hinted
     *       rows;</li>
     *   <li>otherwise the full dictionary, loaded through the shard
     *       cache.</li>
     * </ol>
     * Should a doc outside the hint still be requested from a sparse
     * instance, the full column is loaded and the doc's term is looked
     * up in the sparse dictionary; a term that is not there has no
     * ordinal in the space the consumer is using, and the instance
     * fails rather than report the doc as missing or reorder the
     * values.
     *
     * <p>The full dictionary is either the heap {@code BytesRef[]} plus
     * {@code int[]} built for this request or a {@link CachedKeywordColumn}
     * of the off-heap store. Over the store, {@link #ordValue} is one
     * read of the ordinal buffer, {@link #lookupOrd} copies the term
     * into a scratch owned by this instance (the returned
     * {@link BytesRef} is valid until the next call, as with Lucene's
     * own codecs), and {@link #lookupTerm} compares the off-heap bytes
     * in place.
     */
    private final class HintedSortedDocValues extends SortedDocValues {
        private final String name;
        private boolean resolved;
        private int[] ords;
        private BytesRef[] terms;
        private CachedKeywordColumn offHeap;
        private BytesRefBuilder scratch;
        private SparseKeyword sparse;
        private int currentOrd = -1;
        private int doc = -1;

        HintedSortedDocValues(String name) {
            this.name = name;
        }

        private void resolve() {
            if (resolved) {
                return;
            }
            resolved = true;
            try {
                int[] hint = hintedOffsets;
                boolean exclusive = hintExclusive;
                if (serveKeywordSparse(name, hint, exclusive)) {
                    sparse = sparseKeywordFor(name, hint);
                    terms = sparse.terms;
                } else {
                    serveHeldKeywordInsteadOfTake(name, hint, exclusive, false);
                    ensureTextLoaded(name);
                    useFullColumn();
                }
            } catch (IOException e) {
                // SortedDocValues.getValueCount / lookupOrd do not declare
                // IOException, so the scan failure surfaces unchecked.
                throw new UncheckedIOException(e);
            }
        }

        /** Point this instance at whichever full dictionary {@link #ensureTextLoaded} published for the leaf. */
        private void useFullColumn() {
            offHeap = offHeapKeywordColumns.get(name);
            if (offHeap != null) {
                scratch = new BytesRefBuilder();
            } else {
                ords = keywordOrds.get(name);
                terms = keywordTerms.get(name);
            }
        }

        /**
         * Ordinal, in the sparse dictionary, of a doc the hint does not
         * cover. Loads the full column (store or heap, this fragment
         * only) to learn the doc's term.
         */
        private int ordOutsideHint(int target) throws IOException {
            ensureTextLoaded(name, false);
            BytesRef term;
            CachedKeywordColumn full = offHeapKeywordColumns.get(name);
            if (full != null) {
                int fullOrd = full.ord(target);
                if (fullOrd < 0) {
                    return -1;
                }
                if (scratch == null) {
                    scratch = new BytesRefBuilder();
                }
                term = full.term(fullOrd, scratch);
            } else {
                int fullOrd = keywordOrds.get(name)[target];
                if (fullOrd < 0) {
                    return -1;
                }
                term = keywordTerms.get(name)[fullOrd];
            }
            int sparseOrd = Arrays.binarySearch(sparse.terms, term);
            if (sparseOrd < 0) {
                throw new IllegalStateException(
                    "doc "
                        + target
                        + " of column "
                        + name
                        + " on fragment "
                        + fragmentId
                        + " was collected although the Lance scorer that hinted the leaf as exclusive did not match it, "
                        + "and its value is not in the sparse dictionary"
                );
            }
            return sparseOrd;
        }

        @Override
        public int ordValue() {
            return currentOrd;
        }

        @Override
        public BytesRef lookupOrd(int ord) {
            resolve();
            return offHeap != null ? offHeap.term(ord, scratch) : terms[ord];
        }

        @Override
        public int lookupTerm(BytesRef key) throws IOException {
            resolve();
            return offHeap != null ? offHeap.lookupTerm(key) : super.lookupTerm(key);
        }

        @Override
        public int getValueCount() {
            resolve();
            return offHeap != null ? offHeap.valueCount() : terms.length;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            if (liveDocs != null && !liveDocs.get(target)) {
                currentOrd = -1;
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                currentOrd = index >= 0 ? sparse.ords[index] : ordOutsideHint(target);
            } else if (offHeap != null) {
                currentOrd = offHeap.ord(target);
            } else {
                currentOrd = ords[target];
            }
            return currentOrd >= 0;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            resolve();
            if (sparse != null) {
                // Only hinted docs can be collected under an exclusive
                // hint, so iteration walks the hinted rows.
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index < 0) {
                    index = -index - 1;
                }
                for (; index < sparse.offsets.length; index++) {
                    int candidate = sparse.offsets[index];
                    if ((liveDocs == null || liveDocs.get(candidate)) && sparse.ords[index] >= 0) {
                        doc = candidate;
                        currentOrd = sparse.ords[index];
                        return doc;
                    }
                }
            } else {
                for (int i = target; i < maxDoc; i++) {
                    if (liveDocs != null && !liveDocs.get(i)) {
                        continue;
                    }
                    int ord = offHeap != null ? offHeap.ord(i) : ords[i];
                    if (ord >= 0) {
                        doc = i;
                        currentOrd = ord;
                        return i;
                    }
                }
            }
            doc = NO_MORE_DOCS;
            currentOrd = -1;
            return doc;
        }

        @Override
        public long cost() {
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
    }

    /**
     * Multi-valued keyword doc values over a List&lt;Utf8&gt; column.
     * Same source selection order and ordinal-space rules as
     * {@link HintedSortedDocValues}. Over the off-heap store the current
     * doc's ordinals are the flat range
     * {@code [rowStart, rowStart + rowCount)} of the
     * {@link CachedKeywordArrayColumn}; over heap they are the doc's
     * {@code int[]}.
     */
    private final class HintedSortedSetDocValues extends SortedSetDocValues {
        private final String name;
        private boolean resolved;
        private int[][] rowOrds;
        private BytesRef[] terms;
        private CachedKeywordArrayColumn offHeap;
        private BytesRefBuilder scratch;
        private SparseKeywordArray sparse;
        private int[] currentRow;
        private int rowStart;
        private int rowCount;
        private int cursor;
        private int doc = -1;

        HintedSortedSetDocValues(String name) {
            this.name = name;
        }

        private void resolve() {
            if (resolved) {
                return;
            }
            resolved = true;
            try {
                int[] hint = hintedOffsets;
                boolean exclusive = hintExclusive;
                if (serveKeywordSparse(name, hint, exclusive)) {
                    sparse = sparseKeywordArrayFor(name, hint);
                    terms = sparse.terms;
                } else {
                    serveHeldKeywordInsteadOfTake(name, hint, exclusive, true);
                    ensureKeywordArrayLoaded(name);
                    useFullColumn();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void useFullColumn() {
            offHeap = offHeapKeywordArrayColumns.get(name);
            if (offHeap != null) {
                scratch = new BytesRefBuilder();
            } else {
                rowOrds = keywordArrayOrds.get(name);
                terms = keywordArrayTerms.get(name);
            }
        }

        /**
         * Ordinals, in the sparse dictionary, of a doc the hint does not
         * cover. Loads the full column (store or heap, this fragment
         * only) to learn the doc's terms.
         */
        private int[] rowOutsideHint(int target) throws IOException {
            ensureKeywordArrayLoaded(name, false);
            CachedKeywordArrayColumn full = offHeapKeywordArrayColumns.get(name);
            int[] row;
            if (full != null) {
                int start = full.rowStart(target);
                int count = full.rowEnd(target) - start;
                if (count == 0) {
                    return null;
                }
                if (scratch == null) {
                    scratch = new BytesRefBuilder();
                }
                row = new int[count];
                for (int i = 0; i < count; i++) {
                    row[i] = sparseOrdOf(target, full.term(full.ordinal(start + i), scratch));
                }
            } else {
                int[] fullRow = keywordArrayOrds.get(name)[target];
                if (fullRow == null) {
                    return null;
                }
                BytesRef[] fullTerms = keywordArrayTerms.get(name);
                row = new int[fullRow.length];
                for (int i = 0; i < fullRow.length; i++) {
                    row[i] = sparseOrdOf(target, fullTerms[fullRow[i]]);
                }
            }
            // Full-column ordinals are ascending and so are their sparse
            // counterparts (both dictionaries sort the same way).
            return row;
        }

        private int sparseOrdOf(int target, BytesRef term) {
            int sparseOrd = Arrays.binarySearch(sparse.terms, term);
            if (sparseOrd < 0) {
                throw new IllegalStateException(
                    "doc "
                        + target
                        + " of column "
                        + name
                        + " on fragment "
                        + fragmentId
                        + " was collected although the Lance scorer that hinted the leaf as exclusive did not match it, "
                        + "and one of its values is not in the sparse dictionary"
                );
            }
            return sparseOrd;
        }

        @Override
        public long nextOrd() {
            // Caller iterates docValueCount() times; no sentinel needed.
            if (currentRow != null) {
                return currentRow[cursor++];
            }
            return offHeap.ordinal(rowStart + cursor++);
        }

        @Override
        public int docValueCount() {
            return currentRow != null ? currentRow.length : rowCount;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            resolve();
            return offHeap != null ? offHeap.term((int) ord, scratch) : terms[(int) ord];
        }

        @Override
        public long lookupTerm(BytesRef key) throws IOException {
            resolve();
            return offHeap != null ? offHeap.lookupTerm(key) : super.lookupTerm(key);
        }

        @Override
        public long getValueCount() {
            resolve();
            return offHeap != null ? offHeap.valueCount() : terms.length;
        }

        /** Point the current doc at the off-heap range of {@code target}; returns whether it has values. */
        private boolean setStoreRow(int target) {
            currentRow = null;
            rowStart = offHeap.rowStart(target);
            rowCount = offHeap.rowEnd(target) - rowStart;
            return rowCount > 0;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            cursor = 0;
            if (liveDocs != null && !liveDocs.get(target)) {
                currentRow = null;
                rowCount = 0;
                return false;
            }
            resolve();
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                currentRow = index >= 0 ? sparse.rowOrds[index] : rowOutsideHint(target);
            } else if (offHeap != null) {
                return setStoreRow(target);
            } else {
                currentRow = rowOrds[target];
            }
            rowCount = currentRow == null ? 0 : currentRow.length;
            return rowCount > 0;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            resolve();
            cursor = 0;
            if (sparse != null) {
                int index = Arrays.binarySearch(sparse.offsets, target);
                if (index < 0) {
                    index = -index - 1;
                }
                for (; index < sparse.offsets.length; index++) {
                    int candidate = sparse.offsets[index];
                    int[] row = sparse.rowOrds[index];
                    if ((liveDocs == null || liveDocs.get(candidate)) && row != null && row.length > 0) {
                        doc = candidate;
                        currentRow = row;
                        rowCount = row.length;
                        return doc;
                    }
                }
            } else if (offHeap != null) {
                for (int i = target; i < maxDoc; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && setStoreRow(i)) {
                        doc = i;
                        return i;
                    }
                }
            } else {
                for (int i = target; i < rowOrds.length; i++) {
                    if ((liveDocs == null || liveDocs.get(i)) && rowOrds[i] != null && rowOrds[i].length > 0) {
                        doc = i;
                        currentRow = rowOrds[i];
                        rowCount = currentRow.length;
                        return i;
                    }
                }
            }
            doc = NO_MORE_DOCS;
            currentRow = null;
            rowCount = 0;
            return doc;
        }

        @Override
        public long cost() {
            if (!resolved) {
                return maxDoc;
            }
            return sparse != null ? sparse.offsets.length : maxDoc;
        }
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
            if (vector instanceof StructVector struct && structColumns.contains(name)) {
                return decodeStructValue(name, struct, i);
            }
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
     * Decode one struct cell of a take-scan batch into an ordered map of
     * child name to decoded child value, recursing into nested structs.
     * Only children the schema surfaced (present in {@link #columnKind}
     * under their dotted path, or a nested struct with at least one such
     * descendant) appear as keys, so an unsupported child is omitted
     * from {@code _source} the same way its mapping entry is. A child
     * that is Arrow null inside a present struct maps to a {@code null}
     * value, which {@link #materialiseStoredFields} renders as an
     * explicit JSON {@code null}; a struct that is itself null returns
     * {@code null} here (an omitted top-level key, a {@code null} child
     * inside an enclosing struct).
     */
    private Object decodeStructValue(String path, StructVector vector, int i) {
        if (vector.isNull(i)) {
            return null;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (FieldVector child : vector.getChildrenFromFields()) {
            String childPath = path + "." + child.getName();
            if (child instanceof StructVector nested) {
                if (hasSurfacedDescendant(childPath)) {
                    out.put(child.getName(), decodeStructValue(childPath, nested, i));
                }
                continue;
            }
            if (!columnKind.containsKey(childPath)) {
                continue;
            }
            out.put(child.getName(), decodeTakeValue(childPath, child, i));
        }
        return out;
    }

    /** Whether any surfaced column sits under {@code path} (a nested struct with at least one supported leaf). */
    private boolean hasSurfacedDescendant(String path) {
        String prefix = path + ".";
        for (String column : columnKind.keySet()) {
            if (column.startsWith(prefix)) {
                return true;
            }
        }
        return false;
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
                    writeSourceField(builder, name, name, value);
                }
                builder.endObject();
                byte[] json = org.opensearch.core.common.bytes.BytesReference.toBytes(
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder)
                );
                visitor.binaryField(sourceInfo, json);
            }
        }
    }

    /**
     * Render one {@code _source} value under {@code key}. {@code path}
     * is the dotted column path ({@code key} for top-level columns, the
     * full {@code parent.child} path inside a struct) so the
     * {@link ColumnKind} and {@link NumericPrecision} lookups resolve.
     * A {@link java.util.Map} value (a decoded struct) renders as a
     * JSON object, recursing per child; a {@code null} value renders as
     * an explicit JSON {@code null} (only struct children reach here as
     * {@code null} — the top-level loop skips absent columns, keeping
     * their keys out of {@code _source} as before).
     */
    private void writeSourceField(org.opensearch.core.xcontent.XContentBuilder builder, String path, String key, Object value)
        throws IOException {
        if (value == null) {
            builder.nullField(key);
            return;
        }
        if (value instanceof Map<?, ?> struct) {
            builder.startObject(key);
            for (Map.Entry<?, ?> entry : struct.entrySet()) {
                String childName = (String) entry.getKey();
                writeSourceField(builder, path + "." + childName, childName, entry.getValue());
            }
            builder.endObject();
            return;
        }
        switch (columnKind.get(path)) {
            case NUMERIC -> {
                long numericValue = (Long) value;
                NumericPrecision precision = numericPrecision.getOrDefault(path, NumericPrecision.INTEGER);
                switch (precision) {
                    case FLOAT -> builder.field(key, org.apache.lucene.util.NumericUtils.sortableIntToFloat((int) numericValue));
                    case DOUBLE -> builder.field(key, org.apache.lucene.util.NumericUtils.sortableLongToDouble(numericValue));
                    case INTEGER -> {
                        if (pkType == org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.UNSIGNED_LONG
                            && path.equals(fieldName)) {
                            // UInt64 PK column: emit as an unsigned
                            // decimal so the JSON number matches
                            // what the operator wrote. Other
                            // UInt64 columns are not surfaced by
                            // classify(), so this branch fires
                            // only for the PK.
                            builder.field(key, new java.math.BigInteger(Long.toUnsignedString(numericValue)));
                        } else {
                            builder.field(key, numericValue);
                        }
                    }
                }
            }
            case BOOLEAN -> builder.field(key, (Boolean) value);
            case TEXT_FTS, TEXT_KEYWORD -> builder.field(key, (String) value);
            case KEYWORD_ARRAY -> builder.field(key, (String[]) value);
            case BINARY -> builder.field(key, Base64.getEncoder().encodeToString((byte[]) value));
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

    /**
     * Whether every wrapper between {@code reader} and the underlying
     * {@code LanceFragmentLeafReader} is one of the plugin's own
     * ({@link LanceSequentialLeafReader}) or OpenSearch's
     * ({@link OpenSearchLeafReader}). Any other {@link FilterLeafReader}
     * in the chain is a reader wrapper installed by another plugin,
     * such as the security plugin's document and field level security
     * reader, which hides rows and fields the Lance scan still returns.
     * A sparse keyword dictionary built from all the hit rows would then
     * expose, through its value count and the global ordinal map, terms
     * that live only in hidden rows, so the Lance scorers do not mark
     * their hint exclusive on such a leaf and the keyword dictionaries
     * stay on the full column path. Returns {@code false} when the
     * chain does not end in a {@code LanceFragmentLeafReader}.
     */
    public static boolean wrappedOnlyByOwnReaders(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader filter) {
            if (!(current instanceof LanceSequentialLeafReader) && !(current instanceof OpenSearchLeafReader)) {
                return false;
            }
            current = filter.getDelegate();
        }
        return current instanceof LanceFragmentLeafReader;
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
        DirectoryReader bridge;
        synchronized (this) {
            closed = true;
            bridge = cacheLifetimeBridge;
        }
        // Close outside the monitor so the bridge's closed listeners
        // (cache invalidation callbacks) do not run while holding it.
        if (bridge != null) {
            bridge.close();
        }
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return cacheLifetimeBridge().leaves().get(0).reader().getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheLifetimeBridge().getReaderCacheHelper();
    }
}
