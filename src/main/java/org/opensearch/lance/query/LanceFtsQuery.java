/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceCircuitBreaker;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * Lucene query that executes a Lance {@link FullTextQuery} tree per leaf
 * (fragment) and streams back matching docids with Lance BM25 scores.
 * This is the RFC's shard-level rewrite of text queries to native Lance
 * queries, surfaced through the real search phase.
 *
 * <p>The query carries the {@link FullTextQuery} tree Lance should
 * evaluate together with the set of columns it references. The column
 * set drives the FLS visibility check that runs once per leaf: if the
 * security plugin has hidden any referenced column from the wrapper
 * reader, the query contributes no hits (matching the standard
 * {@code match} behaviour on a Lucene index).
 */
public final class LanceFtsQuery extends Query {

    /** Sentinel that disables top-k pushdown; the scan is bounded only by fragment maxDoc. */
    public static final int SCAN_LIMIT_UNBOUNDED = 0;

    /** Default for {@link #subsetProbeLimit()}. */
    public static final int DEFAULT_SUBSET_PROBE_LIMIT = 1_000_000;

    /** Default for {@link #subsetProbeRatio()}. */
    public static final double DEFAULT_SUBSET_PROBE_RATIO = 0.03d;

    /** Default for {@link #subsetProbeMinRows()}. */
    public static final int DEFAULT_SUBSET_PROBE_MIN_ROWS = 10_000;

    /**
     * Absolute cap on the rows an executor that holds a proper subset
     * of the table's fragments reads from the probe scan of an
     * unbounded FTS shape (see {@link #effectiveSubsetProbeLimit}).
     * Runtime parameter, not query identity: two queries that differ
     * only in the probe limit in force return the same rows, so it is
     * not part of {@link #equals}. Written by the plugin from the
     * {@code lance.fts.subset_probe_limit} cluster setting, read by
     * every scan. The two companions below come from
     * {@code lance.fts.subset_probe_ratio} and
     * {@code lance.fts.subset_probe_min_rows} the same way.
     */
    private static volatile int subsetProbeLimit = DEFAULT_SUBSET_PROBE_LIMIT;
    private static volatile double subsetProbeRatio = DEFAULT_SUBSET_PROBE_RATIO;
    private static volatile int subsetProbeMinRows = DEFAULT_SUBSET_PROBE_MIN_ROWS;

    /** Current value of the {@code lance.fts.subset_probe_limit} setting. */
    public static int subsetProbeLimit() {
        return subsetProbeLimit;
    }

    /** Install a new probe cap; the next scan picks it up. */
    public static void setSubsetProbeLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("subset probe limit must be at least 1, was " + limit);
        }
        subsetProbeLimit = limit;
    }

    /** Current value of the {@code lance.fts.subset_probe_ratio} setting. */
    public static double subsetProbeRatio() {
        return subsetProbeRatio;
    }

    /** Install a new probe ratio (0.0 to 1.0); the next scan picks it up. */
    public static void setSubsetProbeRatio(double ratio) {
        if (Double.isNaN(ratio) || ratio < 0d || ratio > 1d) {
            throw new IllegalArgumentException("subset probe ratio must be between 0.0 and 1.0, was " + ratio);
        }
        subsetProbeRatio = ratio;
    }

    /** Current value of the {@code lance.fts.subset_probe_min_rows} setting. */
    public static int subsetProbeMinRows() {
        return subsetProbeMinRows;
    }

    /** Install a new probe floor; the next scan picks it up. */
    public static void setSubsetProbeMinRows(int rows) {
        if (rows < 1) {
            throw new IllegalArgumentException("subset probe min rows must be at least 1, was " + rows);
        }
        subsetProbeMinRows = rows;
    }

    /**
     * Rows the probe scan of an unbounded FTS shape may return before
     * an executor that covers {@code subsetRows} rows of the table
     * gives it up and repeats the scan restricted to its fragments:
     * {@code min(subsetProbeLimit, max(subsetProbeMinRows,
     * floor(subsetRows * subsetProbeRatio)))}.
     *
     * <p>The ratio balances the two ways a subset executor can pay for
     * an unbounded scan. The probe scans the whole table and the
     * executor receives every match, keeps its own rows and drops the
     * rest; its extra cost over a whole table executor is the received
     * rows, about 0.5 to 0.9 µs per row on the measured hardware (rows
     * with {@code _rowaddr} and {@code _score} transferred through
     * Arrow and bucketed by fragment). The restricted scan makes Lance
     * read {@code _rowid} over the executor's fragments as a prefilter
     * before the index lookup; its extra cost grows with the rows the
     * executor covers, about 21 ns per row (139 ms for 6.7M rows at
     * 20M). The two are equal when the matches are about 3 percent of
     * the covered rows, which is the default ratio: below it the probe
     * is the cheaper path, above it the restricted scan is. The floor
     * keeps small tables and one hit queries on the probe, where the
     * prefilter read would be the larger cost in relative terms, and
     * the cap bounds the heap the probe rows take on one node.
     */
    public static long effectiveSubsetProbeLimit(long subsetRows) {
        long proportional = (long) Math.floor(Math.max(0L, subsetRows) * subsetProbeRatio);
        return Math.min((long) subsetProbeLimit, Math.max((long) subsetProbeMinRows, proportional));
    }

    private final FullTextQuery fullTextQuery;
    private final Set<String> columns;
    private final String canonical;
    /**
     * Upper bound on rows the per-leaf Lance FTS scan is allowed to
     * return. {@link #SCAN_LIMIT_UNBOUNDED} lets the scan produce
     * every matching row (the historical behaviour).
     *
     * <p>Callers that only need the top {@code size} hits (pure FTS
     * shape with no sort, no aggregations, no post_filter) pass that
     * value here and let Lance's inverted-index scorer stop scoring
     * once it has enough score-sorted rows. This is the direct
     * counterpart of {@link
     * org.opensearch.lance.query.LanceScanFilterQuery#scanLimit()}
     * for FTS: {@code LanceScanFilterQuery} clips at the level of
     * "matched row addresses" for scalar filters, this class clips
     * at "score-sorted rows" for full-text queries.
     *
     * <p>Aggregations, sort by a non-score field, and post_filter all
     * need the full matched set; the fragment path resolver keeps
     * this at {@link #SCAN_LIMIT_UNBOUNDED} for those shapes.
     */
    private final int scanLimit;
    /**
     * Optional Lance SQL predicate the FTS scan evaluates before the
     * inverted-index lookup, or {@code null} for an unfiltered scan.
     *
     * <p>When set, {@code ensureShardScan} passes it as
     * {@code ScanOptions.Builder.filter(sql).prefilter(true)}, which
     * makes Lance restrict the posting-list lookup to the rows the
     * predicate selects (through the column's scalar index when one
     * exists, otherwise through a filtered read of {@code _rowid}).
     * The fragment path resolver fills this in for
     * {@code bool { must: [one FTS clause], filter: [...], must_not:
     * [...] }} so the scalar clauses never have to be evaluated on the
     * Lucene side against every FTS hit, and {@code scanLimit} then
     * bounds the already filtered result.
     */
    private final String prefilterSql;

    /**
     * Primary constructor. {@code columns} is the set of Lance columns
     * the {@code fullTextQuery} references transitively. All of them
     * must be visible on the leaf reader (i.e., not hidden by FLS)
     * for the query to run against that leaf.
     */
    public LanceFtsQuery(FullTextQuery fullTextQuery, Set<String> columns) {
        this(fullTextQuery, columns, SCAN_LIMIT_UNBOUNDED);
    }

    /**
     * Constructor including the top-k pushdown hint and no prefilter.
     */
    public LanceFtsQuery(FullTextQuery fullTextQuery, Set<String> columns, int scanLimit) {
        this(fullTextQuery, columns, scanLimit, null);
    }

    /**
     * Full constructor including the top-k pushdown hint and the
     * optional Lance SQL prefilter ({@code null} for none).
     */
    public LanceFtsQuery(FullTextQuery fullTextQuery, Set<String> columns, int scanLimit, String prefilterSql) {
        this.fullTextQuery = Objects.requireNonNull(fullTextQuery, "fullTextQuery must not be null");
        this.columns = Set.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
        if (this.columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        if (scanLimit < 0) {
            throw new IllegalArgumentException("scanLimit must not be negative, was " + scanLimit);
        }
        this.scanLimit = scanLimit;
        this.prefilterSql = prefilterSql;
        this.canonical = canonicalString(fullTextQuery);
    }

    /**
     * Return a copy of this query with a new scan limit. Used by the
     * fragment path resolver to attach the request's {@code size} to
     * a plain-DSL LanceFtsQuery without going through every DSL builder.
     */
    public LanceFtsQuery withScanLimit(int newScanLimit) {
        if (newScanLimit == scanLimit) {
            return this;
        }
        return new LanceFtsQuery(fullTextQuery, columns, newScanLimit, prefilterSql);
    }

    /**
     * Return a copy of this query whose Lance scan is prefiltered by
     * {@code newPrefilterSql} ({@code null} removes the prefilter).
     */
    public LanceFtsQuery withPrefilterSql(String newPrefilterSql) {
        if (Objects.equals(newPrefilterSql, prefilterSql)) {
            return this;
        }
        return new LanceFtsQuery(fullTextQuery, columns, scanLimit, newPrefilterSql);
    }

    public int scanLimit() {
        return scanLimit;
    }

    /**
     * The Lance SQL prefilter applied to the FTS scan, or {@code null}
     * when the scan is unfiltered.
     */
    public String prefilterSql() {
        return prefilterSql;
    }

    /**
     * Convenience for a simple {@code match} query (single column,
     * default operator, no fuzziness).
     */
    public LanceFtsQuery(String column, String text) {
        this(FullTextQuery.match(text, column), Set.of(column));
    }

    /**
     * Convenience for a {@code match} query with an explicit operator.
     */
    public LanceFtsQuery(String column, String text, FullTextQuery.Operator operator) {
        this(
            FullTextQuery.match(text, column, 1f, Optional.empty(), 50, operator == null ? FullTextQuery.Operator.OR : operator, 0),
            Set.of(column)
        );
    }

    /**
     * Convenience for a match / phrase query switch. Kept for the two
     * existing single-column DSLs.
     */
    public LanceFtsQuery(String column, String text, boolean phrase, int slop) {
        this(column, text, phrase, slop, FullTextQuery.Operator.OR);
    }

    /**
     * Convenience for a match / phrase query with explicit operator.
     * {@code operator} is ignored when {@code phrase} is true.
     */
    public LanceFtsQuery(String column, String text, boolean phrase, int slop, FullTextQuery.Operator operator) {
        this(
            phrase
                ? FullTextQuery.phrase(text, column, Math.max(0, slop))
                : FullTextQuery.match(text, column, 1f, Optional.empty(), 50, operator == null ? FullTextQuery.Operator.OR : operator, 0),
            Set.of(column)
        );
    }

    public FullTextQuery fullTextQuery() {
        return fullTextQuery;
    }

    Set<String> columns() {
        return columns;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new LanceFtsWeight(this, boost);
    }

    /**
     * Weight for a Lance FTS query. Runs a single Lance scan across
     * every Lance-backed leaf in the shard (the "shard-level scan"
     * pattern that {@code LanceKnnQuery.LanceKnnWeight} already uses
     * for nearest-neighbour queries), buckets the hits by fragment
     * id from the returned {@code _rowaddr}, and hands each per-leaf
     * {@link ScorerSupplier} its fragment's slice out of the shared
     * map.
     *
     * <p>The reason for the shard-level scan is fragment fixed cost.
     * Each {@code dataset.newScan} costs 15-19 ms on a 20M-row table
     * regardless of how much it returns, so one scan per fragment
     * over 80 fragments spends 1.2-1.5 s before any matching work.
     * One newScan per Weight instance makes the fixed cost
     * independent of fragment count.
     *
     * <p>The scan never carries a {@code fragmentIds} restriction,
     * also when the reader's leaves are a proper subset of the
     * table's fragments (one executor of a multi node fan out). Lance
     * plans a filtered read of {@code _rowid} over every listed
     * fragment as soon as the list is set, and that read grows with
     * the row count while the index lookup itself does not; see
     * {@link LanceFtsQuery#restrictToFragmentsUnlessAll}. Instead the
     * scan runs over the whole table and rows whose fragment id (the
     * upper 32 bits of {@code _rowaddr}) is not one of the reader's
     * fragments are dropped here. The two shapes:
     *
     * <ul>
     *   <li>Bounded ({@code scanLimit} set, the pure top k shape):
     *       one scan with {@code limit(scanLimit)} over the table.
     *       Every executor computes the same global top k and keeps
     *       its own fragments' rows out of it. The fragments are
     *       partitioned over the executors, so the per executor
     *       results are disjoint and their union is exactly the
     *       global top k; the coordinator's k way merge of those
     *       lists is the same top k a single executor holding the
     *       whole table would return. The only slack is a tie in
     *       score at rank k, where Lance's tie break decides which of
     *       the tied rows is inside the k rows.</li>
     *   <li>Unbounded (aggregations, sort by a field, post_filter,
     *       {@code size 0}): a probe scan with
     *       {@code limit(effectiveSubsetProbeLimit(rows covered))}
     *       over the table. When Lance
     *       returns fewer rows than the probe limit every match has
     *       been seen and the reader's rows are kept. When the probe
     *       fills up the match set is too large to filter here, the
     *       probe rows are discarded and the scan is repeated with the
     *       {@code fragmentIds} restriction, which transfers the
     *       executor's matches only at the price of the
     *       {@code _rowid} prefilter read. Probe rows are never mixed
     *       with the restricted rows: the restricted scan already
     *       contains them and the union would double count.</li>
     * </ul>
     * A reader that holds every fragment skips the filter and keeps
     * the single unrestricted scan it always ran.
     *
     * <p>The class is public so the fragment executor, which creates
     * the Weight itself and drives hits and aggregations through it,
     * can read {@link #hitCount()} and {@link #complete()} afterwards
     * instead of running a second Lance scan to count the matches,
     * and can deliver the hit sets to the fragment readers through
     * {@link #hintExclusive} before it builds aggregators and sort
     * comparators.
     */
    public final class LanceFtsWeight extends Weight implements LanceHintingWeight {

        private final float boost;
        // Result of the shard scan, populated by the first Lance-backed
        // leaf we visit and reused for every other leaf in the same
        // Weight. Set once via CAS so concurrent readers see a fully
        // constructed map.
        private final AtomicReference<ShardScan> shardScan = new AtomicReference<>();

        LanceFtsWeight(LanceFtsQuery query, float boost) {
            super(query);
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return Explanation.match(0f, "lance fts");
        }

        /**
         * Whether every column the query references is present on the
         * leaf's reader. Security plugin FLS hides a field by dropping
         * it from the wrapper reader's FieldInfos; if any referenced
         * column is missing on this leaf's wrapper reader, the query
         * contributes no hits so the caller cannot use it as a probe
         * against the hidden data. The check stays per-leaf (rather
         * than moving into ensureShardScan) so an FLS decision that
         * hides the column on one leaf still leaves other leaves
         * working.
         */
        private boolean columnsVisible(LeafReaderContext context) {
            for (String col : query().columns()) {
                if (context.reader().getFieldInfos().fieldInfo(col) == null) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Hits of the shard-level scan that fall into {@code leaf}'s
         * fragment, running the scan on first use. A leaf without hits
         * gets {@link LanceFragmentHits#EMPTY} rather than null: it
         * still receives a supplier over an empty hit set, so Lucene
         * asks it for a BulkScorer exactly when it would have for a
         * leaf with hits, and the leaf learns that nothing will be
         * collected on it; a keyword terms aggregation built after the
         * hits phase can then skip the leaf's dictionary instead of
         * loading it for the global ordinal map.
         */
        private LanceFragmentHits leafHits(LeafReaderContext context, LanceFragmentLeafReader leaf) throws IOException {
            LanceFragmentHits hits = ensureShardScan(context, leaf).get(leaf.fragmentId());
            return hits == null ? LanceFragmentHits.EMPTY : hits;
        }

        @Override
        public void hintExclusive(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null || !columnsVisible(context)) {
                return;
            }
            leaf.hintMatchedOffsets(
                leafHits(context, leaf).sortedDocIds(),
                LanceFragmentLeafReader.wrappedOnlyByOwnReaders(context.reader())
            );
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null || !columnsVisible(context)) {
                return null;
            }
            LanceFragmentHits hits = leafHits(context, leaf);
            // Sorted by offset so the Lucene DocIdSetIterator contract
            // (ascending docIds) is satisfied; the arrays are the ones
            // every other supplier and hint of this Weight sees for
            // the fragment.
            int[] docIds = hits.sortedDocIds();
            float[] hitScores = hits.sortedScores();
            // Tell the leaf which rows this Weight matched so a sort or
            // aggregation column can be fetched for those rows alone;
            // the supplier below upgrades the hint to exclusive when
            // Lucene lets this Weight drive collection on the leaf.
            leaf.hintMatchedOffsets(docIds, false);
            LanceSparseHitIterator iterator = new LanceSparseHitIterator(docIds, hitScores, docIds.length);
            Scorer scorer = new Scorer() {
                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }

                @Override
                public float getMaxScore(int upTo) {
                    return Float.MAX_VALUE;
                }

                @Override
                public float score() {
                    return iterator.currentScore();
                }

                @Override
                public int docID() {
                    return iterator.docID();
                }
            };
            // Under a reader wrapper the plugin does not know (the security
            // plugin's document and field level security reader) the
            // hint is delivered but never marked exclusive, so keyword
            // dictionaries keep the full column path there.
            return new LanceHintingScorerSupplier(scorer, leaf, docIds, LanceFragmentLeafReader.wrappedOnlyByOwnReaders(context.reader()));
        }

        private LanceFtsQuery query() {
            return (LanceFtsQuery) getQuery();
        }

        /**
         * Number of rows of the shard-level scan that belong to the
         * fragments the enclosing reader exposes, or {@code -1} when
         * no leaf of this Weight has been scored yet and the scan has
         * therefore not run. Rows Lance returned for other fragments
         * of the table (the scan runs unrestricted, see the class
         * javadoc) are not counted. The value is the executor's exact
         * match count only when {@link #complete()} is true: a
         * bounded scan that filled its limit may have left matches of
         * the reader's fragments unseen, and the number of rows kept
         * says nothing about how many were cut off. Hits dropped on a
         * leaf by the FLS column check in {@link #scorerSupplier} are
         * still included, so the executor only relies on this count
         * when no reader wrapper is installed.
         */
        public long hitCount() {
            ShardScan scan = shardScan.get();
            if (scan == null) {
                return -1L;
            }
            long total = 0L;
            for (LanceFragmentHits fragmentHits : scan.hits().values()) {
                total += fragmentHits.size();
            }
            return total;
        }

        /**
         * Whether the shard-level scan saw every match of the reader's
         * fragments, so {@link #hitCount()} is the exact match count.
         * True for an unbounded scan (the probe came back short of its
         * limit, or the restricted scan ran after it filled up) and
         * for a bounded scan that returned fewer rows than its limit
         * before any filtering; false for a bounded scan that filled
         * its limit, and before the scan has run.
         */
        public boolean complete() {
            ShardScan scan = shardScan.get();
            return scan != null && scan.complete();
        }

        /**
         * The {@link ScanOptions} of every Lance scan this Weight has
         * issued, in order; empty before the first leaf is scored.
         * For tests of the scan plan.
         */
        List<ScanOptions> issuedScans() {
            ShardScan scan = shardScan.get();
            return scan == null ? List.of() : scan.scans();
        }

        private Map<Integer, LanceFragmentHits> ensureShardScan(LeafReaderContext context, LanceFragmentLeafReader leaf)
            throws IOException {
            ShardScan cached = shardScan.get();
            if (cached != null) {
                return cached.hits();
            }
            // First scan on this shard is where Lance loads the
            // inverted index into native memory. Refuse to start it
            // if the breaker has already tripped so we do not push
            // the cache past its budget mid-query.
            LanceCircuitBreaker.checkAndTrip("lance_fts_query");

            // Collect the fragment ids of every Lance-backed leaf in
            // this shard. They decide which rows of the unrestricted
            // scan below are kept, and whether the filter is needed
            // at all (a reader that holds every fragment keeps every
            // row). The IndexSearcher built by the fragment
            // coordinator wraps exactly those fragments' leaves
            // inside a LanceDirectoryReader, so walking the top-level
            // context's leaves() gives the same subset the request
            // was fanned out with — no more, no less. Walk up to the
            // top-level context because LeafReaderContext.leaves()
            // (inherited from IndexReaderContext) is only valid when
            // isTopLevel is true.
            IndexReaderContext topCtx = context;
            while (!topCtx.isTopLevel) {
                topCtx = topCtx.parent;
            }
            Set<Integer> fragmentIds = new LinkedHashSet<>();
            long subsetRows = 0L;
            for (LeafReaderContext sibling : topCtx.leaves()) {
                LanceFragmentLeafReader sl = LanceFragmentLeafReader.unwrap(sibling.reader());
                if (sl != null) {
                    fragmentIds.add(sl.fragmentId());
                    subsetRows += sl.maxDoc();
                }
            }
            if (fragmentIds.isEmpty()) {
                // No Lance-backed leaves at all: nothing to scan.
                // Install an empty result so subsequent scorer calls
                // short-circuit through the cache.
                shardScan.compareAndSet(null, new ShardScan(new HashMap<>(), true, List.of()));
                return shardScan.get().hits();
            }

            Dataset dataset = leaf.dataset();
            // Rows of fragments outside the reader are dropped from
            // the unrestricted scan; a reader over the whole table
            // has nothing to drop.
            Set<Integer> keep = coversAllFragments(fragmentIds, dataset) ? null : fragmentIds;
            List<ScanOptions> issued = new ArrayList<>(2);
            Map<Integer, LanceFragmentHits> hits = new HashMap<>();
            boolean complete;
            if (scanLimit != SCAN_LIMIT_UNBOUNDED) {
                // Top k over the whole table; each executor keeps its
                // share of the same k rows. Lance rejects limit == 0;
                // if a caller passed scanLimit == 0 through some other
                // route the clip is 1.
                long limit = Math.max(1L, (long) scanLimit);
                ScanOptions options = newScanOptions().limit(limit).build();
                issued.add(options);
                long returned = collectHits(dataset, options, keep, hits);
                // Lance returns exactly min(limit, matches) rows, so a
                // short result means every match of the table, and
                // with it every match of the reader's fragments, has
                // been seen.
                complete = returned < limit;
            } else if (keep == null) {
                ScanOptions options = newScanOptions().build();
                issued.add(options);
                collectHits(dataset, options, null, hits);
                complete = true;
            } else {
                // Probe limit proportional to the rows the reader
                // covers, see effectiveSubsetProbeLimit.
                long probeLimit = effectiveSubsetProbeLimit(subsetRows);
                ScanOptions probe = newScanOptions().limit(probeLimit).build();
                issued.add(probe);
                long returned = collectHits(dataset, probe, keep, hits);
                if (returned >= probeLimit) {
                    // Too many matches to filter here: discard the
                    // probe and let Lance restrict the scan to the
                    // reader's fragments.
                    hits = new HashMap<>();
                    ScanOptions restricted = restrictToFragmentsUnlessAll(newScanOptions(), new ArrayList<>(fragmentIds), dataset).build();
                    issued.add(restricted);
                    collectHits(dataset, restricted, null, hits);
                }
                complete = true;
            }
            ShardScan fresh = new ShardScan(hits, complete, List.copyOf(issued));
            // Whichever thread wins the CAS installs the result; losers reuse it.
            if (shardScan.compareAndSet(null, fresh)) {
                return fresh.hits();
            }
            return shardScan.get().hits();
        }

        /**
         * Scan options shared by every scan of this Weight: the
         * full-text query, the row address the hits are bucketed by,
         * and the SQL prefilter of a collapsed bool query. The
         * prefilter runs as a Lance prefilter: the planner evaluates
         * it first (scalar index or filtered _rowid read) and hands
         * the resulting row set to the inverted-index lookup, so a
         * limit clips the already filtered hits.
         */
        private ScanOptions.Builder newScanOptions() {
            ScanOptions.Builder builder = new ScanOptions.Builder().fullTextQuery(query().fullTextQuery()).withRowAddress(true);
            String prefilterSql = query().prefilterSql();
            if (prefilterSql != null) {
                builder = builder.filter(prefilterSql).prefilter(true);
            }
            return builder;
        }

        /**
         * Run {@code options} against {@code dataset} and add every
         * returned row whose fragment id is in {@code keep} (every
         * row when {@code keep} is null) to {@code into}, bucketed by
         * fragment id. Returns the number of rows Lance returned
         * before the filter, which is what a limit is compared with.
         */
        private long collectHits(Dataset dataset, ScanOptions options, Set<Integer> keep, Map<Integer, LanceFragmentHits> into)
            throws IOException {
            long returned = 0L;
            try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    Float4Vector score = (Float4Vector) root.getVector("_score");
                    int rows = root.getRowCount();
                    returned += rows;
                    for (int i = 0; i < rows; i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        if (keep != null && !keep.contains(fragId)) {
                            continue;
                        }
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        float s = score.get(i) * boost;
                        into.computeIfAbsent(fragId, id -> new LanceFragmentHits()).add(offset, s);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                // The Weight contract allows IOException only. Keep the
                // Lance exception as the cause: the fragment executor
                // reads an IllegalArgumentException (Lance's invalid
                // input, such as a phrase query on an index without
                // positions) back out of the chain to answer 400.
                throw new IOException(e);
            }
            return returned;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

    /**
     * Outcome of a Weight's shard-level scan: the hits of the reader's
     * fragments, whether every match of those fragments was seen, and
     * the scans that were issued to get there.
     */
    private record ShardScan(Map<Integer, LanceFragmentHits> hits, boolean complete, List<ScanOptions> scans) {
    }

    @Override
    public String toString(String field) {
        if (prefilterSql == null) {
            return "LanceFtsQuery(" + canonical + ")";
        }
        return "LanceFtsQuery(" + canonical + ",prefilter=" + prefilterSql + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LanceFtsQuery q
            && columns.equals(q.columns)
            && canonical.equals(q.canonical)
            && scanLimit == q.scanLimit
            && Objects.equals(prefilterSql, q.prefilterSql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, canonical, scanLimit, prefilterSql);
    }

    /**
     * Canonical string representation of a {@link FullTextQuery} tree.
     * Only {@link FullTextQuery.MatchQuery}, {@link FullTextQuery.PhraseQuery},
     * and {@link FullTextQuery.MultiMatchQuery} override
     * {@link Object#toString}; {@link FullTextQuery.BoostQuery} and
     * {@link FullTextQuery.BooleanQuery} format their children via
     * {@code Object.toString} which brings identity strings into the mix.
     * We recurse ourselves so the string is stable across instances and
     * usable as an equality key.
     */
    static String canonicalString(FullTextQuery q) {
        switch (q.getType()) {
            case MATCH: {
                FullTextQuery.MatchQuery m = (FullTextQuery.MatchQuery) q;
                return "match{column="
                    + m.getColumn()
                    + ",text="
                    + m.getQueryText()
                    + ",boost="
                    + m.getBoost()
                    + ",fuzziness="
                    + m.getFuzziness()
                    + ",maxExpansions="
                    + m.getMaxExpansions()
                    + ",operator="
                    + m.getOperator()
                    + ",prefixLength="
                    + m.getPrefixLength()
                    + "}";
            }
            case MATCH_PHRASE: {
                FullTextQuery.PhraseQuery p = (FullTextQuery.PhraseQuery) q;
                return "phrase{column=" + p.getColumn() + ",text=" + p.getQueryText() + ",slop=" + p.getSlop() + "}";
            }
            case MULTI_MATCH: {
                FullTextQuery.MultiMatchQuery mm = (FullTextQuery.MultiMatchQuery) q;
                return "multiMatch{columns="
                    + mm.getColumns()
                    + ",text="
                    + mm.getQueryText()
                    + ",boosts="
                    + mm.getBoosts()
                    + ",operator="
                    + mm.getOperator()
                    + "}";
            }
            case BOOST: {
                FullTextQuery.BoostQuery b = (FullTextQuery.BoostQuery) q;
                return "boost{positive="
                    + canonicalString(b.getPositive())
                    + ",negative="
                    + canonicalString(b.getNegative())
                    + ",negativeBoost="
                    + b.getNegativeBoost()
                    + "}";
            }
            case BOOLEAN: {
                FullTextQuery.BooleanQuery bq = (FullTextQuery.BooleanQuery) q;
                StringBuilder sb = new StringBuilder("bool{clauses=[");
                List<FullTextQuery.BooleanClause> clauses = bq.getClauses();
                for (int i = 0; i < clauses.size(); i++) {
                    FullTextQuery.BooleanClause c = clauses.get(i);
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(c.getOccur()).append("=").append(canonicalString(c.getQuery()));
                }
                sb.append("]}");
                return sb.toString();
            }
            default:
                return q.toString();
        }
    }

    /**
     * Whether {@code executorFragmentIds} contains every fragment of
     * {@code dataset}, so an FTS scan restricted to those ids would
     * touch the same rows as an unrestricted scan.
     *
     * <p>Checked as set inclusion ({@code executor ⊇ dataset}) rather
     * than by comparing sizes: the executor list comes from the leaves
     * of the reader the coordinator built while the dataset's fragment
     * list comes from the manifest version the reader pinned, and the
     * two can legitimately disagree in membership (a fragment the
     * coordinator saw that a later compaction removed) with equal
     * sizes. Ids in {@code executorFragmentIds} that the dataset does
     * not know are ignored: they cannot add rows to the scan.
     */
    public static boolean coversAllFragments(Collection<Integer> executorFragmentIds, Dataset dataset) {
        Set<Integer> executor = new HashSet<>(executorFragmentIds);
        for (Fragment fragment : dataset.getFragments()) {
            if (!executor.contains(fragment.getId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Restrict {@code builder} to {@code fragmentIds} only when they are
     * a proper subset of the dataset's fragments.
     *
     * <p>Lance's planner ({@code Scanner::prefilter_source} in
     * {@code rust/lance/src/dataset/scanner.rs}) skips the prefilter
     * stage only when there is no filter and no explicit fragment list.
     * As soon as {@code fragmentIds} is set it plans a filtered read of
     * the {@code _rowid} column over every target fragment and feeds
     * that as a prefilter into the inverted-index lookup, so the scan
     * reads one row id per row in the target fragments before it
     * touches the posting lists, and the query latency grows with the
     * row count of those fragments while the index lookup itself stays
     * constant. Leaving {@code fragmentIds} unset gives Lance the same
     * plan pylance gets ({@code PreFilterSource::None}) and the lookup
     * runs from the index alone.
     *
     * <p>Because of that cost the FTS paths avoid the restriction even
     * for a proper subset: the hits scan and the count scan run over
     * the whole table and keep the rows of the executor's fragments by
     * the fragment id in {@code _rowaddr} ({@link LanceFtsWeight} and
     * the fragment executor's count path explain why the per executor
     * results still merge to the same answer). This method is what
     * those paths fall back to when an unbounded scan matches more
     * rows than the probe limit, so the restriction is paid only when
     * the alternative would transfer that many rows of other
     * executors' fragments. A Lance-side change that builds the
     * prefilter from the fragment bitmap instead of a row id read
     * would make the restricted scan an index-only lookup as well.
     */
    public static ScanOptions.Builder restrictToFragmentsUnlessAll(
        ScanOptions.Builder builder,
        List<Integer> fragmentIds,
        Dataset dataset
    ) {
        if (coversAllFragments(fragmentIds, dataset)) {
            return builder;
        }
        return builder.fragmentIds(fragmentIds);
    }

    /**
     * Walk a {@link FullTextQuery} tree and collect every column it
     * references. Handy for building the {@code columns} parameter of
     * {@link #LanceFtsQuery(FullTextQuery, Set)} from a nested tree.
     */
    public static Set<String> collectColumns(FullTextQuery q) {
        Set<String> out = new LinkedHashSet<>();
        collectColumns(q, out);
        return out;
    }

    private static void collectColumns(FullTextQuery q, Set<String> out) {
        switch (q.getType()) {
            case MATCH:
                out.add(((FullTextQuery.MatchQuery) q).getColumn());
                return;
            case MATCH_PHRASE:
                out.add(((FullTextQuery.PhraseQuery) q).getColumn());
                return;
            case MULTI_MATCH:
                out.addAll(((FullTextQuery.MultiMatchQuery) q).getColumns());
                return;
            case BOOST: {
                FullTextQuery.BoostQuery b = (FullTextQuery.BoostQuery) q;
                collectColumns(b.getPositive(), out);
                collectColumns(b.getNegative(), out);
                return;
            }
            case BOOLEAN: {
                FullTextQuery.BooleanQuery bq = (FullTextQuery.BooleanQuery) q;
                for (FullTextQuery.BooleanClause c : bq.getClauses()) {
                    collectColumns(c.getQuery(), out);
                }
                return;
            }
            default:
        }
    }
}
