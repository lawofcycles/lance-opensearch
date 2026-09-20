/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
     * Per-fragment result buckets for a single shard-wide Lance FTS
     * scan. Sizes are typically at most the caller's scan limit for
     * the fragments that carry hits (many fragments carry none), so
     * the arrays grow geometrically from a small initial capacity.
     *
     * <p>Modelled on {@link LanceKnnQuery.FragmentHits} but stores
     * BM25 scores (higher = better) rather than distances (lower =
     * better). Kept as a separate class instead of shared because
     * the field names {@code offsets}/{@code scores} read better in
     * the FTS context than {@code offsets}/{@code distances}.
     */
    static final class FtsFragmentHits {
        int[] offsets = new int[8];
        float[] scores = new float[offsets.length];
        int size = 0;

        void add(int offset, float score) {
            if (size == offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
                scores = Arrays.copyOf(scores, scores.length * 2);
            }
            offsets[size] = offset;
            scores[size] = score;
            size++;
        }
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
     * <p>{@code scanLimit} caps the total transfer across the fragment
     * subset, so a request of {@code size=20} over 20 fragments per
     * node transfers 20 hits, not 400, matching the shape the
     * coordinator merges anyway.
     *
     * <p>The class is public so the fragment executor, which creates
     * the Weight itself and drives hits and aggregations through it,
     * can read {@link #hitCount()} afterwards instead of running a
     * second Lance scan to count the matches.
     */
    public final class LanceFtsWeight extends Weight {

        private final float boost;
        // Cache populated by the first Lance-backed leaf we visit and
        // reused for every other leaf in the same Weight. Set once
        // via CAS so concurrent readers see a fully constructed map.
        private final AtomicReference<Map<Integer, FtsFragmentHits>> shardHits = new AtomicReference<>();

        LanceFtsWeight(LanceFtsQuery query, float boost) {
            super(query);
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return Explanation.match(0f, "lance fts");
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            if (!(org.apache.lucene.index.FilterLeafReader.unwrap(context.reader()) instanceof LanceFragmentLeafReader leaf)) {
                return null;
            }
            // Security plugin FLS hides a field by dropping it from
            // the wrapper reader's FieldInfos. If any referenced
            // column is missing on this leaf's wrapper reader,
            // contribute no hits so the caller cannot use the FTS
            // query as a probe against the hidden data. The check
            // stays per-leaf (rather than moving into ensureShardScan)
            // so an FLS decision that hides the column on one leaf
            // still leaves other leaves working.
            for (String col : query().columns()) {
                if (context.reader().getFieldInfos().fieldInfo(col) == null) {
                    return null;
                }
            }
            Map<Integer, FtsFragmentHits> hitsByFragment = ensureShardScan(context, leaf);
            FtsFragmentHits hits = hitsByFragment.get(leaf.fragmentId());
            // A leaf without hits still gets a supplier (over an empty
            // hit set) rather than null. Lucene then asks it for a
            // BulkScorer exactly when it would have for a leaf with
            // hits, and the leaf learns that nothing will be collected
            // on it; a keyword terms aggregation built after the hits
            // phase can then skip the leaf's dictionary instead of
            // loading it for the global ordinal map.
            int hitCount = hits == null ? 0 : hits.size;
            // Pack (offset, score) into longs sorted by offset so
            // the Lucene DocIdSetIterator contract (ascending docIds)
            // is satisfied. Offsets are non-negative ints so signed
            // long ordering is docId-ascending.
            long[] packed = new long[hitCount];
            for (int i = 0; i < hitCount; i++) {
                int offset = hits.offsets[i];
                float s = hits.scores[i];
                packed[i] = ((long) offset << 32) | (Float.floatToIntBits(s) & 0xFFFFFFFFL);
            }
            Arrays.sort(packed);
            int[] docIds = new int[hitCount];
            float[] hitScores = new float[hitCount];
            for (int i = 0; i < hitCount; i++) {
                docIds[i] = (int) (packed[i] >>> 32);
                hitScores[i] = Float.intBitsToFloat((int) (packed[i] & 0xFFFFFFFFL));
            }
            // Tell the leaf which rows this Weight matched so a sort or
            // aggregation column can be fetched for those rows alone;
            // the supplier below upgrades the hint to exclusive when
            // Lucene lets this Weight drive collection on the leaf.
            leaf.hintMatchedOffsets(docIds, false);
            LanceSparseHitIterator iterator = new LanceSparseHitIterator(docIds, hitScores, hitCount);
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
         * Number of rows the shard-level scan returned across every
         * fragment, or {@code -1} when no leaf of this Weight has been
         * scored yet and the scan has therefore not run. The value
         * covers the fragments the enclosing reader exposes (the scan
         * is restricted to them unless they are the whole table), and
         * is bounded by {@link LanceFtsQuery#scanLimit()} when that is
         * set: at the limit the true match count may be higher. Hits
         * dropped on a leaf by the FLS column check in
         * {@link #scorerSupplier} are still included, so the executor
         * only relies on this count when no reader wrapper is
         * installed.
         */
        public long hitCount() {
            Map<Integer, FtsFragmentHits> hits = shardHits.get();
            if (hits == null) {
                return -1L;
            }
            long total = 0L;
            for (FtsFragmentHits fragmentHits : hits.values()) {
                total += fragmentHits.size;
            }
            return total;
        }

        private Map<Integer, FtsFragmentHits> ensureShardScan(LeafReaderContext context, LanceFragmentLeafReader leaf) throws IOException {
            Map<Integer, FtsFragmentHits> cached = shardHits.get();
            if (cached != null) {
                return cached;
            }
            // First scan on this shard is where Lance loads the
            // inverted index into native memory. Refuse to start it
            // if the breaker has already tripped so we do not push
            // the cache past its budget mid-query.
            LanceCircuitBreaker.checkAndTrip("lance_fts_query");

            // Collect the fragment ids of every Lance-backed leaf in
            // this shard. They decide whether the single scan below
            // needs a fragmentIds restriction: when the leaves cover
            // every fragment of the dataset none is passed, otherwise
            // the scan is limited to the fragments this per-node
            // executor was assigned. The IndexSearcher built by the
            // fragment coordinator wraps exactly those fragments'
            // leaves inside a LanceDirectoryReader, so walking the
            // top-level context's leaves() gives the same subset the
            // request was fanned out with — no more, no less. Walk up
            // to the top-level context because
            // LeafReaderContext.leaves() (inherited from
            // IndexReaderContext) is only valid when isTopLevel is
            // true.
            org.apache.lucene.index.IndexReaderContext topCtx = context;
            while (!topCtx.isTopLevel) {
                topCtx = topCtx.parent;
            }
            List<Integer> fragmentIds = new ArrayList<>();
            for (LeafReaderContext sibling : topCtx.leaves()) {
                LanceFragmentLeafReader sl = LanceFragmentLeafReader.unwrap(sibling.reader());
                if (sl != null) {
                    fragmentIds.add(sl.fragmentId());
                }
            }
            if (fragmentIds.isEmpty()) {
                // No Lance-backed leaves at all: nothing to scan.
                // Install an empty map so subsequent scorer calls
                // short-circuit through the cache.
                Map<Integer, FtsFragmentHits> empty = new HashMap<>();
                shardHits.compareAndSet(null, empty);
                return shardHits.get();
            }

            // effectiveLimit applies to the whole scan (not per
            // fragment). scanLimit == SCAN_LIMIT_UNBOUNDED asks for
            // every match; otherwise Lance stops after that many
            // score-sorted rows across the fragment subset. Callers
            // that need every match (aggregation, sort by non-score,
            // post_filter) keep the sentinel via
            // TransportLanceFragmentQueryAction.resolveScanFilterTopK.
            long effectiveLimit;
            if (scanLimit == SCAN_LIMIT_UNBOUNDED) {
                effectiveLimit = 0L; // Lance treats 0 as "no limit"
            } else {
                // Lance rejects limit == 0; if a caller passed
                // scanLimit == 0 through some other route the top-k
                // clip below is 1.
                effectiveLimit = Math.max(1L, (long) scanLimit);
            }

            // Restrict the scan to the executor's fragments only when
            // they are a proper subset of the table; a full set is
            // scanned without fragmentIds so Lance does not plan a
            // _rowid prefilter read over the whole table before the
            // inverted-index lookup (see restrictToFragmentsUnlessAll).
            ScanOptions.Builder builder = restrictToFragmentsUnlessAll(new ScanOptions.Builder(), fragmentIds, leaf.dataset())
                .fullTextQuery(query().fullTextQuery())
                .withRowAddress(true);
            // A scalar predicate pushed down from a bool query runs
            // as a Lance prefilter: the planner evaluates it first
            // (scalar index or filtered _rowid read) and hands the
            // resulting row set to the inverted-index lookup, so the
            // limit below clips the already filtered hits.
            String prefilterSql = query().prefilterSql();
            if (prefilterSql != null) {
                builder = builder.filter(prefilterSql).prefilter(true);
            }
            if (effectiveLimit > 0) {
                builder = builder.limit(effectiveLimit);
            }

            Map<Integer, FtsFragmentHits> fresh = new HashMap<>();
            try (LanceScanner scanner = leaf.dataset().newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    Float4Vector score = (Float4Vector) root.getVector("_score");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        int offset = (int) (addr & 0xFFFFFFFFL);
                        float s = score.get(i) * boost;
                        fresh.computeIfAbsent(fragId, id -> new FtsFragmentHits()).add(offset, s);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            // Whichever thread wins the CAS installs the map; losers reuse it.
            if (shardHits.compareAndSet(null, fresh)) {
                return fresh;
            }
            return shardHits.get();
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
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
    static boolean coversAllFragments(Collection<Integer> executorFragmentIds, Dataset dataset) {
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
     * touches the posting lists. On a table where the executor holds
     * every fragment that read covers the whole table and the query
     * latency grows with the row count while the index lookup itself
     * stays constant. Leaving {@code fragmentIds} unset in that case
     * gives Lance the same plan pylance gets ({@code PreFilterSource::None})
     * and the lookup runs from the index alone.
     *
     * <p>When the executor holds a proper subset the ids are still
     * passed and the prefilter read still happens over that subset;
     * the alternative (scan without {@code fragmentIds} and drop
     * foreign rows in Java) is not taken because the bounded
     * {@code limit} would then count rows that belong to other
     * executors and the top-k per executor would be wrong, and
     * without {@code limit} every match would be transferred. A
     * Lance-side change that builds the prefilter from the fragment
     * bitmap instead of a row id read would make the subset case an
     * index-only lookup as well.
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
