/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
     * Primary constructor. {@code columns} is the set of Lance columns
     * the {@code fullTextQuery} references transitively. All of them
     * must be visible on the leaf reader (i.e., not hidden by FLS)
     * for the query to run against that leaf.
     */
    public LanceFtsQuery(FullTextQuery fullTextQuery, Set<String> columns) {
        this(fullTextQuery, columns, SCAN_LIMIT_UNBOUNDED);
    }

    /**
     * Full constructor including the top-k pushdown hint.
     */
    public LanceFtsQuery(FullTextQuery fullTextQuery, Set<String> columns, int scanLimit) {
        this.fullTextQuery = Objects.requireNonNull(fullTextQuery, "fullTextQuery must not be null");
        this.columns = Set.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
        if (this.columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        if (scanLimit < 0) {
            throw new IllegalArgumentException("scanLimit must not be negative, was " + scanLimit);
        }
        this.scanLimit = scanLimit;
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
        return new LanceFtsQuery(fullTextQuery, columns, newScanLimit);
    }

    public int scanLimit() {
        return scanLimit;
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

    FullTextQuery fullTextQuery() {
        return fullTextQuery;
    }

    Set<String> columns() {
        return columns;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.match(0f, "lance fts");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                if (!(org.apache.lucene.index.FilterLeafReader.unwrap(context.reader()) instanceof LanceFragmentLeafReader leaf)) {
                    return null;
                }
                // Security plugin FLS hides a field by dropping it from the
                // wrapper reader's FieldInfos. If any referenced column is
                // missing on the wrapper reader, contribute no hits so the
                // caller cannot use it as a search term either.
                for (String col : columns) {
                    if (context.reader().getFieldInfos().fieldInfo(col) == null) {
                        return null;
                    }
                }
                // Bail out early if the shared native Session has caught
                // up to the configured limit: dataset.newScan below is
                // exactly the call that loads the inverted index into
                // native memory, so running it after the breaker trips
                // would be the growth path we are trying to prevent.
                LanceCircuitBreaker.checkAndTrip("lance_fts_query");
                int maxDoc = leaf.maxDoc();
                // Clip the FTS scan at the caller-supplied top-k when
                // one was pinned. Lance's inverted-index scorer
                // maintains a bounded heap of score-sorted rows, so
                // asking for k << maxDoc costs O(hits * log k) rather
                // than O(hits) with a full transfer. The scan floor
                // is 1 (Lance rejects limit == 0); shapes that must
                // see every match keep the sentinel and get maxDoc.
                long effectiveLimit = scanLimit == SCAN_LIMIT_UNBOUNDED ? (long) maxDoc : Math.min((long) scanLimit, (long) maxDoc);
                // Accumulate hits into a packed long array
                // (docId << 32 | Float.floatToIntBits(score)) sized
                // to the effective limit rather than the fragment's
                // maxDoc. A fragment with maxDoc = 250,000 and a
                // size:10 query used to allocate 1 MB of scores and
                // 31 KB of bitset per query; the packed form uses
                // 8 bytes * hitCount, an order of magnitude smaller
                // in the common case and unchanged when the caller
                // asks for the full match set. Growth is capped by
                // effectiveLimit so an unbounded scan cannot overrun.
                int capacity = (int) Math.min(effectiveLimit, 128L);
                if (capacity <= 0) {
                    capacity = 1;
                }
                long[] packed = new long[capacity];
                int hitCount = 0;
                ScanOptions options = new ScanOptions.Builder().fragmentIds(Collections.singletonList(leaf.fragmentId()))
                    .fullTextQuery(fullTextQuery)
                    .withRowAddress(true)
                    .limit(effectiveLimit)
                    .build();
                try (LanceScanner scanner = leaf.dataset().newScan(options); ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                        Float4Vector score = (Float4Vector) root.getVector("_score");
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (hitCount == packed.length) {
                                int nextSize = Math.min(packed.length * 2, Math.max((int) effectiveLimit, packed.length + 1));
                                packed = java.util.Arrays.copyOf(packed, nextSize);
                            }
                            int offset = (int) (rowAddr.get(i) & 0xFFFFFFFFL);
                            float s = score.get(i) * boost;
                            packed[hitCount++] = ((long) offset << 32) | (Float.floatToIntBits(s) & 0xFFFFFFFFL);
                        }
                    }
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException(e);
                }
                if (hitCount == 0) {
                    return null;
                }
                // Sort by packed key so docIds land ascending; scores
                // ride along in the low 32 bits. Java Arrays.sort on
                // long uses the natural (signed) ordering, but offsets
                // are non-negative int values, so the high 32 bits
                // stay in 0..Integer.MAX_VALUE and the sort is
                // effectively docId ascending, score ascending as a
                // stable secondary — the Lucene DocIdSetIterator
                // contract asks for ascending docIds and does not
                // care about score order at equal docIds.
                java.util.Arrays.sort(packed, 0, hitCount);
                int[] docIds = new int[hitCount];
                float[] hitScores = new float[hitCount];
                for (int i = 0; i < hitCount; i++) {
                    docIds[i] = (int) (packed[i] >>> 32);
                    hitScores[i] = Float.intBitsToFloat((int) (packed[i] & 0xFFFFFFFFL));
                }
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
                return new Weight.DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "LanceFtsQuery(" + canonical + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LanceFtsQuery q && columns.equals(q.columns) && canonical.equals(q.canonical) && scanLimit == q.scanLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, canonical, scanLimit);
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
