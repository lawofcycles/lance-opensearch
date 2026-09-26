/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;

/**
 * Column loading for one {@link LanceFragmentLeafReader}: turns Lance
 * columns of the leaf's fragment into the offset indexed arrays,
 * presence bitmaps and keyword dictionaries the doc values read.
 *
 * <p>Owns, per leaf, the request scoped heap columns (numeric, boolean,
 * geo_point, keyword, keyword array) loaded lazily by the
 * {@code ensureXxxLoaded} family, the off-heap {@link ColumnStore}
 * entries the {@link LanceShardColumnCache} publishes for the fragment,
 * the hint a Lance-side scorer reported together with the sparse
 * structures taken for it ({@link SparseNumeric}, {@link SparseKeyword},
 * {@link SparseKeywordArray}), the per column monitors that serialise a
 * load, and the request breaker accounting of single fragment heap
 * loads. Every scan pins the fragment, projects one column, asks for
 * row addresses and layers the top-level {@link #filterSql} in where
 * the load is a full column scan.
 *
 * <p>Does not own the {@code LeafReader} contract, the doc id layout or
 * the doc values themselves: it hands out loaded columns and the doc
 * values decide which source to read. The static Arrow decoders
 * ({@link #readAsLong}, {@link #decodeGeoPoint},
 * {@link #internListElements}) are shared with the shard cache, the
 * off-heap store and the stored fields path so every path agrees on the
 * long encoding of a cell.
 */
final class LanceColumnLoader {

    private static final Logger LOGGER = LogManager.getLogger(LanceColumnLoader.class);

    /** Leaf this loader serves; the shard cache keys its publishes by it and the hinted take maps its doc ids. */
    private final LanceFragmentLeafReader leaf;
    private final Dataset dataset;
    private final int fragmentId;
    /** The leaf's {@code maxDoc}: rows plus nested elements. Column arrays are sized by it and indexed by physical row. */
    private final int maxDoc;
    /** Shared, immutable description of the table's columns; see {@link LanceFragmentSchema}. */
    private final LanceFragmentSchema schema;
    /**
     * Sub-field name → base column name lookup for multi-fields, from
     * the schema; {@link #scanColumnFor} consults it for the raw-string
     * view of an ip column's keyword sub-field.
     */
    private final Map<String, String> keywordSubFields;
    /**
     * SQL predicate the caller wants applied to every per-column scan
     * this leaf issues, or {@code null} for an unfiltered scan.
     *
     * <p>Fragment path queries whose top-level shape is a scalar
     * filter the query planner can print as Lance SQL ship the
     * predicate as
     * {@code LanceFragmentQueryRequest.filterSql()}, and the fragment
     * dispatch handler forwards it here so the lazy column loads
     * inside {@link #ensureNumericLoaded} et al. only materialise the
     * rows that match. Without the predicate, {@code filter + terms}
     * or {@code filter + sum} reads every value in the aggregated
     * column even though the Weight already filters.
     *
     * <p>Kept {@code null} when the query cannot be expressed in
     * Lance SQL (FTS, knn, unsupported shapes) so the loader falls
     * back to full-column scans and the aggregator still runs on
     * every doc the Weight yields.
     */
    private final String filterSql;
    // Per-column monitors so ensureXxxLoaded serialises the Lance scan for
    // that column without blocking other columns. The first accessor pays the
    // scan cost, subsequent readers see the populated map entry via the
    // ConcurrentHashMap happens-before edge.
    private final Map<String, Object> columnLocks = new ConcurrentHashMap<>();
    // Per-column data. Populated lazily by ensureXxxLoaded; ConcurrentHashMap
    // provides the visibility guarantee for the writer / reader pair.
    final Map<String, long[]> numericColumns = new ConcurrentHashMap<>();
    // Per-column presence bitmap; bit set = value present, bit clear = Arrow null.
    // NumericDocValues.advanceExact and _source materialisation both consult this
    // to distinguish "value is 0" from "value is missing" for nullable Arrow columns.
    final Map<String, FixedBitSet> numericPresence = new ConcurrentHashMap<>();
    final Map<String, long[]> booleanColumns = new ConcurrentHashMap<>();
    final Map<String, FixedBitSet> booleanPresence = new ConcurrentHashMap<>();
    /**
     * geo_point-overridden columns of this fragment: one encoded long
     * per row, {@code (encodeLatitude(lat) << 32) | (encodeLongitude(lon)
     * & 0xFFFFFFFFL)} — the exact {@code LatLonDocValuesField} layout
     * OpenSearch's geo queries, {@code _geo_distance} sort and geo
     * aggregations decode. Loaded by {@link #ensureGeoPointLoaded} in
     * one Lance scan of the Struct's two children (or the
     * FixedSizeList's element vector); presence bit clear when the cell
     * or either component is Arrow null or out of coordinate range.
     */
    final Map<String, long[]> geoColumns = new ConcurrentHashMap<>();
    final Map<String, FixedBitSet> geoPresence = new ConcurrentHashMap<>();
    /**
     * Per geo column: encoded bounds and present-row count
     * ({@code minLat, maxLat, minLon, maxLon, count} as
     * {@code GeoEncodingUtils} ints), collected during the load so
     * {@code getPointValues} can answer the root cell of its flat
     * point tree without rescanning the array.
     */
    final Map<String, int[]> geoBounds = new ConcurrentHashMap<>();
    /** How each geo_point column stores its point (Struct child names or FixedSizeList element order), from the schema. */
    private final Map<String, LanceFragmentSchema.GeoPointColumn> geoPointColumns;
    /**
     * Numeric and boolean columns of this fragment served from the
     * off-heap {@link ColumnStore}, published by
     * {@link LanceShardColumnCache} through {@link #publishOffHeapColumn}.
     * A column present here is complete for every physical row, so it
     * takes the place of the heap arrays above; the two never hold the
     * same column at once because the shard cache tries the store first
     * and only loads into heap when the store has no room.
     */
    final Map<String, CachedColumn> offHeapColumns = new ConcurrentHashMap<>();
    // Utf8 columns without an FTS index surface as keyword. Their sorted
    // term dictionary plus per-doc ordinals let getSortedDocValues serve
    // term, terms, aggregation and sort requests through the doc value
    // path. No per-row String copy is kept: _source is rendered from the
    // per-hit take, and Binary columns have no doc value representation
    // at all so they are never loaded through the reader.
    final Map<String, BytesRef[]> keywordTerms = new ConcurrentHashMap<>();
    final Map<String, int[]> keywordOrds = new ConcurrentHashMap<>();
    // List<Utf8> columns surface as multi-valued keyword; keywordArrayOrds
    // and keywordArrayTerms back a multi-valued SortedSetDocValues.
    final Map<String, int[][]> keywordArrayOrds = new ConcurrentHashMap<>();
    final Map<String, BytesRef[]> keywordArrayTerms = new ConcurrentHashMap<>();
    /**
     * Keyword columns of this fragment served from the off-heap
     * {@link ColumnStore} (dictionary plus per-row ordinals), published
     * by {@link LanceShardColumnCache} through
     * {@link #publishOffHeapKeywordColumn}. Same exclusivity with the
     * heap maps above as {@link #offHeapColumns}: the shard cache tries
     * the store first and loads into heap only when the store has no
     * room or the reader carries a top-level filter.
     */
    final Map<String, CachedKeywordColumn> offHeapKeywordColumns = new ConcurrentHashMap<>();
    /** Multi-valued counterpart of {@link #offHeapKeywordColumns}. */
    final Map<String, CachedKeywordArrayColumn> offHeapKeywordArrayColumns = new ConcurrentHashMap<>();

    /**
     * Largest fraction of the leaf's {@code maxDoc} a hinted hit set may cover
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
     * {@code HintedNumericDocValues.resolve}). Kept as a single
     * constant so the threshold can be tuned in one place.
     */
    static final double SPARSE_RATIO = 0.0025;

    /**
     * Request-scoped hit set for this leaf, or {@code null} when no
     * Lance-side scorer has reported one. Sorted ascending doc ids
     * (physical row offsets, or their parent doc ids when the table has
     * nested columns) that the last {@link #hintMatchedOffsets}
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
    volatile int[] hintedOffsets;
    /**
     * Whether the Lance-side scorer that delivered {@link #hintedOffsets}
     * has shown that every doc the current search collects on this
     * leaf is one of the hinted docs (its own {@code BulkScorer} drives
     * collection). Ordinal-based doc values ({@code getSortedDocValues},
     * {@code getSortedSetDocValues}) build their sparse term
     * dictionary only under this flag, because an ordinal space cannot
     * be widened after a consumer has observed it. Numeric doc values
     * use the hint without the flag and fall back to the full column
     * on the first doc outside the hint.
     */
    volatile boolean hintExclusive;
    /**
     * Sparse numeric and boolean values taken for {@link #hintedOffsets},
     * keyed by column name. Cleared whenever the hint is replaced.
     */
    final Map<String, SparseNumeric> sparseNumeric = new ConcurrentHashMap<>();
    /** Sparse keyword dictionaries taken for {@link #hintedOffsets}, keyed by base column name. */
    final Map<String, SparseKeyword> sparseKeyword = new ConcurrentHashMap<>();
    /** Sparse multi-valued keyword dictionaries taken for {@link #hintedOffsets}, keyed by column name. */
    final Map<String, SparseKeywordArray> sparseKeywordArray = new ConcurrentHashMap<>();
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
    final Map<String, Boolean> keywordServedSparse = new ConcurrentHashMap<>();

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

    LanceColumnLoader(
        LanceFragmentLeafReader leaf,
        Dataset dataset,
        int fragmentId,
        int maxDoc,
        LanceFragmentSchema schema,
        String filterSql
    ) {
        this.leaf = leaf;
        this.dataset = dataset;
        this.fragmentId = fragmentId;
        this.maxDoc = maxDoc;
        this.schema = schema;
        this.filterSql = filterSql;
        this.keywordSubFields = schema.keywordSubFields();
        this.geoPointColumns = schema.geoPointColumns();
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
    void ensureNumericLoaded(String name) throws IOException {
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
    void ensureNumericLoaded(String name, boolean useShardCache) throws IOException {
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
            if (cache != null && cache.publishFromStoreForLeaf(leaf, name, false)) {
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
     * downstream {@code getSortedNumericDocValues} calls see the
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
     * Attach a {@link LanceShardColumnCache} to this loader's leaf. Called
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

    void ensureBooleanLoaded(String name) throws IOException {
        ensureBooleanLoaded(name, true);
    }

    void ensureBooleanLoaded(String name, boolean useShardCache) throws IOException {
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
            if (cache != null && cache.publishFromStoreForLeaf(leaf, name, true)) {
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
     * Lance-scan a geo_point column into {@link #geoColumns} /
     * {@link #geoPresence}: one scan of the single column (the whole
     * Struct or FixedSizeList projects both components), each present
     * pair encoded into the {@code LatLonDocValuesField} long layout
     * ({@code encodeLatitude(lat) << 32 | encodeLongitude(lon) &
     * 0xFFFFFFFFL}). A row whose cell or either component is Arrow
     * null, or whose coordinates are outside the +/-90 / +/-180 bounds,
     * is served as missing (presence bit clear); out-of-range rows are
     * counted and logged once per fragment, mirroring the unparsable-ip
     * handling. The load is per fragment and charged to the request
     * breaker; the shard cache's consolidated scan does not carry geo
     * columns today.
     */
    void ensureGeoPointLoaded(String name) throws IOException {
        if (geoColumns.containsKey(name)) {
            return;
        }
        synchronized (columnLock(name)) {
            if (geoColumns.containsKey(name)) {
                return;
            }
            LanceShardColumnCache cache = shardColumnCache;
            long heapBytes = LanceShardColumnCache.numericHeapBytes(maxDoc);
            chargeHeap(cache, heapBytes, name);
            boolean published = false;
            try {
                long[] col = new long[maxDoc];
                FixedBitSet presence = new FixedBitSet(maxDoc);
                LanceFragmentSchema.GeoPointColumn spec = geoPointColumns.get(name);
                int outOfRange = 0;
                int minLat = Integer.MAX_VALUE;
                int maxLat = Integer.MIN_VALUE;
                int minLon = Integer.MAX_VALUE;
                int maxLon = Integer.MIN_VALUE;
                int count = 0;
                ScanOptions colOptions = singleColumnScan(name);
                try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        FieldVector vector = root.getVector(name);
                        for (int i = 0; i < root.getRowCount(); i++) {
                            int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                            double[] point = decodeGeoPoint(spec, vector, i);
                            if (point == null) {
                                continue;
                            }
                            try {
                                int latEncoded = GeoEncodingUtils.encodeLatitude(point[0]);
                                int lonEncoded = GeoEncodingUtils.encodeLongitude(point[1]);
                                col[offset] = (((long) latEncoded) << 32) | (lonEncoded & 0xFFFFFFFFL);
                                presence.set(offset);
                                minLat = Math.min(minLat, latEncoded);
                                maxLat = Math.max(maxLat, latEncoded);
                                minLon = Math.min(minLon, lonEncoded);
                                maxLon = Math.max(maxLon, lonEncoded);
                                count++;
                            } catch (IllegalArgumentException outOfBounds) {
                                outOfRange++;
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
                if (outOfRange > 0) {
                    LOGGER.warn(
                        "fragment {}: {} rows of geo_point column [{}] have coordinates outside +/-90 / +/-180 and are served as missing",
                        fragmentId,
                        outOfRange,
                        name
                    );
                }
                geoBounds.put(name, new int[] { minLat, maxLat, minLon, maxLon, count });
                geoPresence.put(name, presence);
                geoColumns.put(name, col);
                published = true;
            } finally {
                if (!published) {
                    releaseHeap(cache, heapBytes);
                }
            }
        }
    }

    /**
     * Decode one geo cell into {@code {lat, lon}}, or {@code null} when
     * the cell or either component is Arrow null. Shared by the column
     * load above and the per-hit take decode so {@code _source} and the
     * doc values agree on what counts as missing.
     */
    static double[] decodeGeoPoint(LanceFragmentSchema.GeoPointColumn spec, FieldVector vector, int i) {
        if (vector == null || vector.isNull(i)) {
            return null;
        }
        if (spec.isStruct()) {
            StructVector struct = (StructVector) vector;
            Float8Vector lat = (Float8Vector) struct.getChild(spec.latChild());
            Float8Vector lon = (Float8Vector) struct.getChild(spec.lonChild());
            if (lat == null || lon == null || lat.isNull(i) || lon.isNull(i)) {
                return null;
            }
            return new double[] { lat.get(i), lon.get(i) };
        }
        FixedSizeListVector list = (FixedSizeListVector) vector;
        Float8Vector element = (Float8Vector) list.getDataVector();
        int base = i * 2;
        if (element.isNull(base) || element.isNull(base + 1)) {
            return null;
        }
        double first = element.get(base);
        double second = element.get(base + 1);
        return spec.latFirst() ? new double[] { first, second } : new double[] { second, first };
    }

    /**
     * Build the keyword dictionary for a Utf8 column: sorted
     * {@code BytesRef[]} terms plus a per-doc ordinal array (-1 for
     * Arrow null), stored in {@link #keywordTerms} / {@link #keywordOrds}
     * for {@code getSortedDocValues}. Called only for
     * {@link ColumnKind#TEXT_KEYWORD} columns and for TEXT_FTS columns
     * that carry a {@code multi_fields} keyword sub-field; nothing else
     * reads Utf8 values through the reader any more ({@code _source}
     * comes from the per-hit take), so no per-row {@link String} copy
     * is retained. Delegates to {@link LanceShardColumnCache} when one
     * is installed so the scan runs once per shard, and so the column is
     * served from the off-heap store when the reader has one.
     */
    void ensureTextLoaded(String name) throws IOException {
        ensureTextLoaded(name, true);
    }

    /**
     * The Lance column a dictionary storage name reads. Almost always
     * the name itself; the keyword sub-field of an {@code ip} column is
     * the exception: its raw-string view is keyed under the sub-field
     * name while the bytes come from the base column's scan. Plain
     * sub-fields never reach the loaders under their own name because
     * {@code getSortedDocValues} routes them to the base column.
     */
    private String scanColumnFor(String name) {
        String base = keywordSubFields.get(name);
        return base != null && schema.ipColumns().contains(base) ? base : name;
    }

    void ensureTextLoaded(String name, boolean useShardCache) throws IOException {
        if (keywordOrds.containsKey(name) || offHeapKeywordColumns.containsKey(name)) {
            return;
        }
        String scanColumn = scanColumnFor(name);
        LanceShardColumnCache cache = shardColumnCache;
        // The raw-string view of an ip column's sub-field stays a
        // per-leaf load: the shard cache and the off-heap store key
        // dictionaries by the Lance column name, which for this view
        // would collide with the encoded dictionary of the base column.
        if (cache != null && useShardCache && scanColumn.equals(name)) {
            cache.loadTextColumn(name);
            return;
        }
        synchronized (columnLock(name)) {
            if (keywordOrds.containsKey(name) || offHeapKeywordColumns.containsKey(name)) {
                return;
            }
            if (cache != null && scanColumn.equals(name) && cache.publishKeywordFromStoreForLeaf(leaf, name)) {
                return;
            }
            long charged = LanceShardColumnCache.intArrayBytes(maxDoc);
            chargeHeap(cache, charged, name);
            boolean published = false;
            try {
                int[] ids = new int[maxDoc];
                Arrays.fill(ids, -1);
                KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder(schema.termEncoder(name));
                ScanOptions colOptions = singleColumnScan(scanColumn);
                try (LanceScanner scanner = dataset.newScan(colOptions); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        VarCharVector vector = (VarCharVector) root.getVector(scanColumn);
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
                IpTermEncoder.logInvalid(name, fragmentId, builder.invalidCount());
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
     * {@code getSortedSetDocValues}.
     */
    void ensureKeywordArrayLoaded(String name) throws IOException {
        ensureKeywordArrayLoaded(name, true);
    }

    void ensureKeywordArrayLoaded(String name, boolean useShardCache) throws IOException {
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
            if (cache != null && cache.publishKeywordArrayFromStoreForLeaf(leaf, name)) {
                return;
            }
            long charged = LanceShardColumnCache.objectArrayBytes(maxDoc);
            chargeHeap(cache, charged, name);
            boolean published = false;
            try {
                int[][] rows = new int[maxDoc][];
                KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder(schema.termEncoder(name));
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
                IpTermEncoder.logInvalid(name, fragmentId, builder.invalidCount());
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
     * Elements the builder's encoder rejects are skipped like nulls.
     * Shared by the per-leaf loader and {@link LanceShardColumnCache}.
     */
    static int[] internListElements(KeywordDictionaryBuilder builder, ListVector vector, VarCharVector elements, int index) {
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        int[] ids = new int[end - start];
        int count = 0;
        for (int e = start; e < end; e++) {
            if (!elements.isNull(e)) {
                int id = builder.intern(elements, e);
                if (id >= 0) {
                    ids[count++] = id;
                }
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
     * Install the hit set a Lance-side scorer reported for this leaf and
     * drop the sparse structures taken for the previous one when it
     * changes. The contract (sorting, replacement, exclusivity) is
     * documented on {@link LanceFragmentLeafReader#hintMatchedOffsets},
     * the public entry point that delegates here.
     */
    void hintMatchedOffsets(int[] sortedOffsets, boolean exclusive) {
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
    boolean isSparseHint(int[] hint) {
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
    boolean serveHeldFromStore(String name, boolean isBoolean) throws IOException {
        LanceShardColumnCache cache = shardColumnCache;
        // No filterSql gate here, unlike the keyword lookup: a numeric
        // store entry holds one value per physical row, so a request
        // that carries a top-level filter still reads exactly its own
        // docs from it. A keyword dictionary built under a filter would
        // have a different ordinal space, which is why
        // storeHoldsKeyword declines when a filter is present.
        return cache != null && cache.storeHoldsColumn(leaf, name) && cache.publishFromStoreForLeaf(leaf, name, isBoolean);
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
        return cache != null && cache.storeHoldsKeyword(leaf, name);
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
    void serveHeldKeywordInsteadOfTake(String name, int[] hint, boolean exclusive, boolean multiValued) throws IOException {
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
        if (cache == null || !cache.storeHoldsKeyword(leaf, name)) {
            return;
        }
        if (multiValued) {
            cache.publishKeywordArrayFromStoreForLeaf(leaf, name);
        } else {
            cache.publishKeywordFromStoreForLeaf(leaf, name);
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
    static final class SparseNumeric {
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
    static final class SparseKeyword {
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
    static final class SparseKeywordArray {
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
     * Same {@code _rowaddr IN (...)} shape and {@link LanceStoredFields#TAKE_CHUNK}
     * chunking as {@link LanceStoredFields#prefetchRows}: Lance turns the predicate into
     * a take by address, so the cost is proportional to the number of
     * offsets, not to the fragment's row count. {@link #filterSql} is
     * not layered in for the same reason as in {@code prefetchRows}:
     * the offsets come from a scan that already applied whatever
     * predicate the query carries. Every scan is counted and timed in
     * {@link FetchTakeStats} under {@link FetchTakeStats.Kind#COLUMN}.
     */
    private void takeHintedRows(String column, int[] sortedOffsets, TakenCellConsumer consumer) throws IOException {
        for (int from = 0; from < sortedOffsets.length; from += LanceStoredFields.TAKE_CHUNK) {
            int to = Math.min(from + LanceStoredFields.TAKE_CHUNK, sortedOffsets.length);
            StringBuilder sql = new StringBuilder((to - from) * 12 + 16).append("_rowaddr IN (");
            for (int i = from; i < to; i++) {
                if (i > from) {
                    sql.append(',');
                }
                // Hint entries are doc ids; the take addresses rows.
                sql.append(((long) fragmentId << 32) | (leaf.rowOf(sortedOffsets[i]) & 0xFFFFFFFFL));
            }
            sql.append(')');
            ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(fragmentId))
                .columns(Collections.singletonList(column))
                .filter(sql.toString())
                .withRowAddress(true)
                .build();
            long start = System.nanoTime();
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    FieldVector vector = root.getVector(column);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                        int hintIndex = Arrays.binarySearch(sortedOffsets, leaf.docOfRow(offset));
                        if (hintIndex >= 0) {
                            consumer.accept(hintIndex, vector, i);
                        }
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                FetchTakeStats.record(FetchTakeStats.Kind.COLUMN, to - from, 1, System.nanoTime() - start, leaf.takeAccumulator());
            }
        }
    }

    /**
     * Sparse values of the numeric or boolean column {@code name} for
     * {@code hint}, taking them on first use. The same instance is
     * returned for every accessor of the column while the hint stands.
     */
    SparseNumeric sparseNumericFor(String name, boolean isBoolean, int[] hint) throws IOException {
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
    SparseKeyword sparseKeywordFor(String name, int[] hint) throws IOException {
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
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder(schema.termEncoder(name));
            takeHintedRows(scanColumnFor(name), hint, (hintIndex, vector, row) -> {
                if (!vector.isNull(row)) {
                    ids[hintIndex] = builder.intern((VarCharVector) vector, row);
                }
            });
            IpTermEncoder.logInvalid(name, fragmentId, builder.invalidCount());
            KeywordDictionaryBuilder.Dictionary dictionary = builder.finish();
            dictionary.remap(ids);
            SparseKeyword fresh = new SparseKeyword(hint, ids, dictionary.terms());
            sparseKeyword.put(name, fresh);
            return fresh;
        }
    }

    /** Sparse multi-valued keyword dictionary of the List&lt;Utf8&gt; column {@code name} for {@code hint}, built on first use. */
    SparseKeywordArray sparseKeywordArrayFor(String name, int[] hint) throws IOException {
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
            KeywordDictionaryBuilder builder = new KeywordDictionaryBuilder(schema.termEncoder(name));
            takeHintedRows(name, hint, (hintIndex, vector, row) -> {
                if (!vector.isNull(row)) {
                    ListVector list = (ListVector) vector;
                    rows[hintIndex] = internListElements(builder, list, (VarCharVector) list.getDataVector(), row);
                }
            });
            IpTermEncoder.logInvalid(name, fragmentId, builder.invalidCount());
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
    boolean serveKeywordSparse(String name, int[] hint, boolean exclusive) {
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

    /**
     * Read an Arrow scalar vector value as a long. Callers guard against
     * nulls; this method assumes the input index has a value. Date and
     * timestamp vectors are normalised to epoch milliseconds so the
     * DateFieldMapper reads them through the same numeric doc value path
     * as integers.
     */
    static long readAsLong(FieldVector v, int i) {
        if (v instanceof TinyIntVector t) {
            return t.get(i);
        }
        if (v instanceof SmallIntVector s) {
            return s.get(i);
        }
        if (v instanceof IntVector iv) {
            return iv.get(i);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(i);
        }
        if (v instanceof UInt8Vector u) {
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
            return NumericUtils.floatToSortableInt(f.get(i));
        }
        if (v instanceof Float8Vector f) {
            // Same shape as Float4Vector but the sortable representation
            // is already 64-bit so no widening is needed. DoubleFieldType
            // decodes with NumericUtils.sortableLongToDouble.
            return NumericUtils.doubleToSortableLong(f.get(i));
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
}
