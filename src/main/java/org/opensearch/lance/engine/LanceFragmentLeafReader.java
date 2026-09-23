/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.lance.Dataset;
import org.lance.ipc.ScanOptions;
import org.opensearch.common.lucene.index.OpenSearchLeafReader;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;

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

    private final int maxDoc;
    private final int numDocs;
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
     * {@link LanceColumnLoader#ensureTextLoaded} consults this set so a TEXT_FTS column
     * with a sub-field still builds the ord data structure that
     * TEXT_KEYWORD would build by default.
     */
    private final java.util.Set<String> basesWithKeywordSub;
    /** Dotted child path → nested column, for children served on child docs. */
    private final Map<String, String> nestedChildToParent;
    /**
     * Doc id layout of this fragment when the schema has nested columns,
     * {@code null} otherwise. When present, {@link #maxDoc} counts rows
     * plus nested elements and every accessor keyed by doc id maps
     * through it; without it doc id == physical row offset as before.
     */
    private final NestedDocLayout nestedLayout;
    /**
     * Doc values, postings and child column loads of the nested layout
     * ({@code _primary_term}, {@code _nested_path}, nested child
     * fields), or {@code null} without nested columns; see
     * {@link NestedDocValues}.
     */
    private final NestedDocValues nestedDocValues;
    /**
     * The {@code _id} / {@code _source} path: per hit row take and
     * rendering, see {@link LanceStoredFields}. Owns the rows taken for
     * this request.
     */
    private final LanceStoredFields storedFields;
    private final Bits liveDocs;
    private final FieldInfos fieldInfos;
    private final Dataset dataset;
    private final int fragmentId;
    /** Shared, immutable description of the table's columns; see {@link LanceFragmentSchema}. */
    private final LanceFragmentSchema schema;
    /**
     * Column loads of this leaf: heap and off-heap columns, the hint a
     * Lance-side scorer reported and the sparse structures taken for
     * it; see {@link LanceColumnLoader}. The doc values read from it.
     */
    private final LanceColumnLoader loader;
    /**
     * Doc values over the loader's columns (hinted numeric, keyword and
     * keyword array, geo_point); see {@link LanceDocValues}. The
     * {@code getXxxDocValues} overrides dispatch on the column kind and
     * build them here.
     */
    private final LanceDocValues docValues;
    /** Hinted hit set ratio above which the doc values load the whole column; see {@link LanceColumnLoader#SPARSE_RATIO}. */
    static final double SPARSE_RATIO = LanceColumnLoader.SPARSE_RATIO;
    // Column kind in schema order, from the shared schema. Preserves schema
    // order so materialiseStoredFields emits _source keys in schema order
    // regardless of which columns have been loaded so far.
    private final Map<String, ColumnKind> columnKind;
    /**
     * Bridge to Lucene's cache lifecycle: the lazily built one-doc
     * reader whose {@code CacheHelper} this leaf exposes as its own, and
     * whose close fires the listeners the OpenSearch caches registered.
     * See {@link LeafCacheBridge} for why it exists and why it is lazy.
     */
    private final LeafCacheBridge cacheBridge = new LeafCacheBridge();

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
     * @param overrides       per-column mapping overrides from the
     *                        index settings, nullable
     * @param ftsColumns      Utf8 columns with an FTS index, from
     *                        {@link #resolveFtsColumns}
     * @param filterSql       Lance SQL predicate every per-column scan
     *                        this leaf issues layers into its
     *                        {@link ScanOptions#filter}, or {@code null}
     *                        for unfiltered scans (see {@link LanceColumnLoader})
     */
    public LanceFragmentLeafReader(
        Dataset dataset,
        int fragmentId,
        long physicalRows,
        boolean hasDeletionFile,
        String intField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        LanceOverrides overrides,
        java.util.Set<String> ftsColumns,
        String filterSql
    ) throws IOException {
        this(
            dataset,
            fragmentId,
            physicalRows,
            hasDeletionFile,
            LanceFragmentSchema.derive(dataset, intField, pkType, overrides, ftsColumns),
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
        this(dataset, fragmentId, resolveFragmentMeta(dataset, fragmentId, physicalRows, hasDeletionFile, schema), schema, filterSql);
    }

    private static LanceWarmCache.FragmentMeta resolveFragmentMeta(
        Dataset dataset,
        int fragmentId,
        long physicalRows,
        boolean hasDeletionFile,
        LanceFragmentSchema schema
    ) throws IOException {
        LanceWarmCache.FragmentMeta meta = new LanceWarmCache.FragmentMeta(fragmentId, physicalRows, hasDeletionFile);
        meta.resolveLiveDocs(dataset);
        meta.resolveNestedLayout(dataset, schema);
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
        this.keywordSubFields = schema.keywordSubFields();
        this.basesWithKeywordSub = schema.basesWithKeywordSub();
        this.nestedChildToParent = schema.nestedChildToParent();
        this.nestedLayout = meta.nestedLayout();
        if (!schema.nestedColumns().isEmpty() && nestedLayout == null) {
            // Fail fast rather than expose a doc id space without the
            // child docs the mapping promises: a nested query on such a
            // leaf would silently match nothing.
            throw new IllegalStateException(
                "fragment " + fragmentId + " was opened without its nested doc layout; call FragmentMeta.resolveNestedLayout first"
            );
        }
        this.columnKind = schema.columnKind();
        if (nestedLayout == null) {
            this.maxDoc = meta.physicalRows();
            this.numDocs = meta.numDocs();
            this.liveDocs = meta.liveDocs();
        } else {
            // Rows plus nested elements; children of deleted rows do not
            // exist in the layout, so only dead parents are masked.
            this.maxDoc = nestedLayout.maxDoc();
            this.numDocs = nestedLayout.numDocs(meta.numDocs());
            this.liveDocs = nestedLayout.docLiveDocs(meta.liveDocs());
        }
        this.fieldInfos = schema.fieldInfos();
        this.loader = new LanceColumnLoader(this, dataset, fragmentId, maxDoc, schema, filterSql);
        this.docValues = new LanceDocValues(this, loader, liveDocs, maxDoc, fragmentId);
        this.storedFields = new LanceStoredFields(this, dataset, fragmentId, schema);
        this.nestedDocValues = nestedLayout == null
            ? null
            : new NestedDocValues(dataset, fragmentId, nestedLayout, liveDocs, maxDoc, schema);
    }

    /**
     * Doc id of the parent doc of physical row {@code row}: the row
     * offset itself without nested columns, the row's parent doc in the
     * {@link NestedDocLayout} otherwise. The boundary every Lance-side
     * hit (a decoded {@code _rowaddr}) crosses to become a Lucene doc id.
     */
    public int docOfRow(int row) {
        return nestedLayout == null ? row : nestedLayout.parentDocOf(row);
    }

    /**
     * Physical row offset behind doc id {@code doc} (the row whose block
     * a child doc belongs to, or the parent's own row). The boundary a
     * Lucene doc id crosses back into Lance row-address space.
     */
    public int rowOf(int doc) {
        return nestedLayout == null ? doc : nestedLayout.rowOfDoc(doc);
    }

    /**
     * Map an ascending array of physical row offsets to parent doc ids.
     * Returns the argument itself without nested columns; the mapping is
     * monotonic, so a sorted input stays sorted.
     */
    public int[] docsOfRows(int[] rows) {
        if (nestedLayout == null) {
            return rows;
        }
        int[] docs = new int[rows.length];
        for (int i = 0; i < rows.length; i++) {
            docs[i] = nestedLayout.parentDocOf(rows[i]);
        }
        return docs;
    }

    /** Child docs this leaf carries beyond its rows (0 without nested columns). */
    public int nestedDocCount() {
        return nestedLayout == null ? 0 : nestedLayout.nestedDocCount();
    }

    /**
     * Row behind {@code doc} when it is a parent doc (every doc without
     * nested columns), or -1 for a nested child doc, which carries no
     * row-scoped values.
     */
    int rowIfParent(int doc) {
        if (nestedLayout == null) {
            return doc;
        }
        return nestedLayout.isParent(doc) ? nestedLayout.rowOfDoc(doc) : -1;
    }

    /** Whether the cache lifetime bridge has been built. Test observability only. */
    boolean cacheLifetimeBridgeExists() {
        return cacheBridge.exists();
    }

    /**
     * The request breaker heap column loads of this leaf are charged to;
     * see {@link LanceColumnLoader#requestBreaker}.
     */
    CircuitBreaker requestBreaker() {
        return loader.requestBreaker();
    }

    /** Publish sink for {@link LanceShardColumnCache#loadNumericColumn}; see {@link LanceColumnLoader#publishNumericColumn}. */
    void publishNumericColumn(String name, long[] values, FixedBitSet presence) {
        loader.publishNumericColumn(name, values, presence);
    }

    /** Publish sink for an off-heap numeric or boolean column; see {@link LanceColumnLoader#publishOffHeapColumn}. */
    void publishOffHeapColumn(String name, CachedColumn column) {
        loader.publishOffHeapColumn(name, column);
    }

    /** Off-heap column of {@code name} published for this leaf, or {@code null}. */
    CachedColumn offHeapColumn(String name) {
        return loader.offHeapColumn(name);
    }

    /** Attach a {@link LanceShardColumnCache} to this leaf; see {@link LanceColumnLoader#setShardColumnCache}. */
    void setShardColumnCache(LanceShardColumnCache cache) {
        loader.setShardColumnCache(cache);
    }

    /** The {@link LanceShardColumnCache} attached to this leaf, or {@code null}; for tests that read its counters. */
    LanceShardColumnCache shardColumnCache() {
        return loader.shardColumnCache();
    }

    /** Publish sink for {@link LanceShardColumnCache#loadBooleanColumn}; see {@link LanceColumnLoader#publishBooleanColumn}. */
    void publishBooleanColumn(String name, long[] values, FixedBitSet presence) {
        loader.publishBooleanColumn(name, values, presence);
    }

    /** Publish sink for {@link LanceShardColumnCache#loadTextColumn}; see {@link LanceColumnLoader#publishTextColumn}. */
    void publishTextColumn(String name, BytesRef[] terms, int[] ords) {
        loader.publishTextColumn(name, terms, ords);
    }

    /** Publish sink for an off-heap keyword dictionary; see {@link LanceColumnLoader#publishOffHeapKeywordColumn}. */
    void publishOffHeapKeywordColumn(String name, CachedKeywordColumn column) {
        loader.publishOffHeapKeywordColumn(name, column);
    }

    /** Multi-valued counterpart of {@link #publishOffHeapKeywordColumn}. */
    void publishOffHeapKeywordArrayColumn(String name, CachedKeywordArrayColumn column) {
        loader.publishOffHeapKeywordArrayColumn(name, column);
    }

    /** Off-heap keyword column of {@code name} published for this leaf, or {@code null}. */
    CachedKeywordColumn offHeapKeywordColumn(String name) {
        return loader.offHeapKeywordColumn(name);
    }

    /** Off-heap keyword array column of {@code name} published for this leaf, or {@code null}. */
    CachedKeywordArrayColumn offHeapKeywordArrayColumn(String name) {
        return loader.offHeapKeywordArrayColumn(name);
    }

    /** Publish sink for {@link LanceShardColumnCache#loadKeywordArrayColumn}; see {@link LanceColumnLoader#publishKeywordArrayColumn}. */
    void publishKeywordArrayColumn(String name, BytesRef[] terms, int[][] rowOrds) {
        loader.publishKeywordArrayColumn(name, terms, rowOrds);
    }

    /**
     * Intern the non-null elements of list {@code index} and return
     * their insertion-order ids; see {@link LanceColumnLoader#internListElements}.
     * Kept here for the shard cache and the off-heap store, which call
     * it through the reader.
     */
    static int[] internListElements(KeywordDictionaryBuilder builder, ListVector vector, VarCharVector elements, int index) {
        return LanceColumnLoader.internListElements(builder, vector, elements, index);
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
        loader.hintMatchedOffsets(sortedOffsets, exclusive);
    }

    /** Current hint, for tests. */
    int[] hintedOffsets() {
        return loader.hintedOffsets();
    }

    /** Whether the current hint is marked exclusive, for tests. */
    boolean hintExclusive() {
        return loader.hintExclusive();
    }

    /** Whether doc values of column {@code name} are currently served from the hinted rows, for tests; see {@link LanceColumnLoader#isServingSparse}. */
    boolean isServingSparse(String name) {
        return loader.isServingSparse(name);
    }

    /** Whether the whole column {@code name} has been materialised on this leaf, for tests; see {@link LanceColumnLoader#isColumnFullyLoaded}. */
    boolean isColumnFullyLoaded(String name) {
        return loader.isColumnFullyLoaded(name);
    }

    /** Whether column {@code name} is served from the off-heap column store on this leaf, for tests. */
    boolean isServingOffHeap(String name) {
        return loader.isServingOffHeap(name);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        if (nestedLayout != null && "_primary_term".equals(field)) {
            // Queries.newNonNestedFilter is a FieldExistsQuery on
            // _primary_term; parents carry it, nested child docs do not.
            return nestedDocValues.parentDocValues();
        }
        String nestedParent = nestedChildToParent.get(field);
        if (nestedParent != null) {
            ColumnKind kind = columnKind.get(field);
            if (kind != ColumnKind.NUMERIC && kind != ColumnKind.BOOLEAN) {
                return null;
            }
            return nestedDocValues.childNumericDocValues(field, nestedParent);
        }
        ColumnKind kind = columnKind.get(field);
        if (kind != ColumnKind.NUMERIC && kind != ColumnKind.BOOLEAN) {
            return null;
        }
        return docValues.numeric(field, kind == ColumnKind.BOOLEAN);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        if (columnKind.get(field) == ColumnKind.GEO_POINT) {
            // One point per row (multi-valued geo points are refused at
            // attach), so the multi-valued interface geo queries, the
            // _geo_distance sort and geo aggregations read is a
            // singleton over the encoded-long column.
            return DocValues.singleton(docValues.geoPoint(field));
        }
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
        if (nestedLayout != null && "_nested_path".equals(field)) {
            // NestedPathFieldMapper.filter is a TermQuery on
            // _nested_path whose term is the nested field's path; serve
            // it from the layout as postings over the child docs.
            return nestedDocValues.nestedPathTerms();
        }
        return null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        return null;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        String nestedParent = nestedChildToParent.get(field);
        if (nestedParent != null) {
            if (columnKind.get(field) != ColumnKind.TEXT_KEYWORD) {
                return null;
            }
            return nestedDocValues.childSortedDocValues(field, nestedParent);
        }
        // A keyword sub-field (multi-fields) resolves to its base column's
        // ord data structure. Route through the base column name so
        // ensureTextLoaded reuses whatever ords were already built for
        // TEXT_KEYWORD, or builds fresh ords for a TEXT_FTS base that
        // otherwise would not have any. The sub-field of an ip column is
        // the exception: the base dictionary holds InetAddressPoint
        // encodings while the sub-field promises the raw strings, so it
        // keeps its own name and the loaders build it a raw per-leaf
        // dictionary from the base column's bytes (see scanColumnFor).
        String source = keywordSubFields.getOrDefault(field, field);
        ColumnKind kind = columnKind.get(source);
        if (kind != ColumnKind.TEXT_KEYWORD && !basesWithKeywordSub.contains(source)) {
            return null;
        }
        if (!field.equals(source) && schema.ipColumns().contains(source)) {
            return docValues.sorted(field);
        }
        return docValues.sorted(source);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        ColumnKind kind = columnKind.get(field);
        if (kind == ColumnKind.KEYWORD_ARRAY) {
            return docValues.sortedSet(field);
        }
        SortedDocValues single = getSortedDocValues(field);
        return single == null ? null : DocValues.singleton(single);
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
    public PointValues getPointValues(String field) throws IOException {
        if (columnKind.get(field) != ColumnKind.GEO_POINT) {
            return null;
        }
        // The stock geo queries (LatLonPoint.newDistanceQuery and
        // friends, wrapped in IndexOrDocValuesQuery by the geo_point
        // field type) require the points side: IndexOrDocValuesQuery's
        // scorerSupplier answers null — no matches — when either leg is
        // missing, and GeoPolygonQueryBuilder builds the LatLonPoint
        // query directly. Serve them a flat, single-cell point tree
        // over the same encoded array the doc values read: the default
        // PointValues.intersect / estimate walk visits every present
        // row against the query's IntersectVisitor, which is exactly
        // the per-hit doc values check with the points API's shape.
        loader.ensureGeoPointLoaded(field);
        int[] bounds = loader.geoBounds.get(field);
        if (bounds[4] == 0) {
            // No present rows: same answer as a segment without points.
            return null;
        }
        return new GeoPointPointValues(this, loader.geoColumns.get(field), loader.geoPresence.get(field), bounds);
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
        return storedFields;
    }

    /**
     * Fetch the rows behind {@code docIds} for {@code _id} / {@code _source};
     * see {@link LanceStoredFields#prefetchRows}.
     */
    public void prefetchRows(int[] docIds) throws IOException {
        storedFields.prefetchRows(docIds);
    }

    /**
     * Materialise stored fields for a single doc; see
     * {@link LanceStoredFields#materialiseStoredFields}.
     */
    public void materialiseStoredFields(int docID, StoredFieldVisitor visitor) throws IOException {
        storedFields.materialiseStoredFields(docID, visitor);
    }

    public Dataset dataset() {
        return dataset;
    }

    public int fragmentId() {
        return fragmentId;
    }

    /** Shared column description of the table; the shard cache reads the dictionary encoders from it. */
    LanceFragmentSchema schema() {
        return schema;
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

    /**
     * Read an Arrow scalar vector value as a long, normalising dates and
     * timestamps to epoch milliseconds and floats to their sortable
     * encoding; see {@link LanceColumnLoader#readAsLong}. Kept here for
     * the shard cache, the off-heap store and the fragment dispatch
     * handler, which call it through the reader.
     */
    public static long readAsLong(FieldVector v, int i) {
        return LanceColumnLoader.readAsLong(v, i);
    }

    @Override
    public void checkIntegrity() {}

    @Override
    protected void doClose() throws IOException {
        cacheBridge.close();
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return cacheBridge.coreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return cacheBridge.readerCacheHelper();
    }
}
