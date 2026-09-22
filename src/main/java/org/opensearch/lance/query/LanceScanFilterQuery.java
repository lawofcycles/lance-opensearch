/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceFragmentLeafReader;

/**
 * Lucene {@link org.apache.lucene.search.Query} that expresses a Lance SQL
 * filter as a Lucene scorer. The Weight runs one Lance native scan per
 * shard against the fragment ids of every Lance-backed leaf under the
 * searcher, with the SQL filter applied, buckets the returned row
 * addresses by fragment, and exposes each fragment's matching offsets as
 * doc ids through a {@link BitSetIterator}. Every returned doc is scored
 * 1.0f (no BM25, no ranking); this class is a bit-mask carrier for use as
 * a filter, not a relevance query.
 *
 * <p>Mirrors {@link LanceFtsQuery}'s shard-level pattern so hits,
 * aggregators and bucket collectors share exactly one Lance native scan
 * per request, and the matched doc set feeds them through the standard
 * {@link org.apache.lucene.search.LeafCollector#collect(int)} contract. The
 * fragment path uses this to run stock aggregators against Lance-native
 * filter results without translating the filter back into Lucene primitives
 * (Lucene has no inverted index or PointValues on top of Lance data).
 *
 * <p>Not {@link org.opensearch.core.common.io.stream.Writeable}: Lucene
 * {@code Query} objects live per-node during a single search cycle and never
 * cross the transport boundary. The coordinator ships {@code filterSql} as a
 * plain string in {@link org.opensearch.lance.dispatch.LanceFragmentQueryRequest};
 * each data node instantiates its own {@code LanceScanFilterQuery} from that
 * string.
 */
public final class LanceScanFilterQuery extends org.apache.lucene.search.Query {

    /** Sentinel that disables top-k pushdown; the scan is bounded only by fragment maxDoc. */
    public static final int SCAN_LIMIT_UNBOUNDED = 0;

    private final String filterSql;
    /**
     * Upper bound on rows the shard-level Lance scan is allowed to return.
     * {@link #SCAN_LIMIT_UNBOUNDED} lets the scan return every match.
     *
     * <p>Callers that know they only need the top {@code size + from}
     * hits (pure scalar filter shape, no sort, no aggregations, no
     * post_filter) pass that value here and let Lance stop scanning
     * once it has enough rows. The scan is deterministic in Lance's
     * internal row-address order (which, for indexed scans, follows
     * the scalar index's ordering); OpenSearch does not promise a
     * particular hit order when the query has no sort clause, so
     * clipping in Lance rather than in Lucene collectors preserves the
     * visible contract while eliminating the Arrow transfer of hits
     * the caller will never look at.
     *
     * <p>Aggregations, sort, and post_filter all need the full matched
     * set; the resolver keeps this at {@link #SCAN_LIMIT_UNBOUNDED} for
     * those shapes.
     */
    private final int scanLimit;

    /**
     * Convenience constructor for callers that do not want to enable
     * top-k pushdown. Equivalent to
     * {@code new LanceScanFilterQuery(filterSql, SCAN_LIMIT_UNBOUNDED)}.
     */
    public LanceScanFilterQuery(String filterSql) {
        this(filterSql, SCAN_LIMIT_UNBOUNDED);
    }

    public LanceScanFilterQuery(String filterSql, int scanLimit) {
        this.filterSql = Objects.requireNonNull(filterSql, "filterSql must not be null");
        if (filterSql.isEmpty()) {
            throw new IllegalArgumentException("filterSql must not be empty; use MatchAllDocsQuery for the null-filter case");
        }
        if (scanLimit < 0) {
            throw new IllegalArgumentException("scanLimit must not be negative, was " + scanLimit);
        }
        this.scanLimit = scanLimit;
    }

    public String filterSql() {
        return filterSql;
    }

    public int scanLimit() {
        return scanLimit;
    }

    @Override
    public String toString(String field) {
        return "LanceScanFilterQuery{filter=" + filterSql + ", scanLimit=" + scanLimit + "}";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o)
            && filterSql.equals(((LanceScanFilterQuery) o).filterSql)
            && scanLimit == ((LanceScanFilterQuery) o).scanLimit;
    }

    @Override
    public int hashCode() {
        return classHash() ^ filterSql.hashCode() ^ Integer.hashCode(scanLimit);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new LanceScanFilterWeight(this, LanceCancellation.of(searcher));
    }

    /**
     * Weight that runs the filter scan once per shard and serves every
     * Lance-backed leaf from the resulting per-fragment bitsets.
     *
     * <p>Same shape as {@code LanceFtsQuery.LanceFtsWeight}: the first
     * leaf the searcher visits collects the fragment ids of every
     * Lance-backed sibling under the top-level reader context, issues
     * one {@code newScan} with {@code fragmentIds} set to that list,
     * buckets the returned {@code _rowaddr}s by fragment, and publishes
     * the map through a CAS so the remaining leaves only look up their
     * own bitset. One scan per shard instead of one per fragment keeps
     * the fixed cost of the filter (plan, index lookup, JNI round
     * trip) from scaling with fragment count.
     *
     * <p>{@code scanLimit} is applied to the shard-level scan, so a
     * bounded request transfers at most {@code scanLimit} row
     * addresses across all fragments, which is exactly the number of
     * hits the caller can return.
     */
    private static final class LanceScanFilterWeight extends Weight {

        private final java.util.concurrent.atomic.AtomicReference<Map<Integer, FixedBitSet>> shardMatches =
            new java.util.concurrent.atomic.AtomicReference<>();
        // Checked at every batch boundary of the filter scan.
        private final LanceCancellation cancellation;

        LanceScanFilterWeight(LanceScanFilterQuery query, LanceCancellation cancellation) {
            super(query);
            this.cancellation = cancellation;
        }

        private LanceScanFilterQuery query() {
            return (LanceScanFilterQuery) getQuery();
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return Explanation.match(1.0f, "lance scan filter");
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(context.reader());
            if (leaf == null) {
                return null;
            }
            Map<Integer, FixedBitSet> matchesByFragment = ensureShardScan(context, leaf);
            FixedBitSet matches = matchesByFragment.get(leaf.fragmentId());
            if (matches == null) {
                return null;
            }
            int cardinality = matches.cardinality();
            if (cardinality == 0) {
                return null;
            }
            DocIdSetIterator iterator = new BitSetIterator(matches, cardinality);
            Scorer scorer = new Scorer() {
                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }

                @Override
                public float getMaxScore(int upTo) {
                    return 1.0f;
                }

                @Override
                public float score() {
                    return 1.0f;
                }

                @Override
                public int docID() {
                    return iterator.docID();
                }
            };
            return new Weight.DefaultScorerSupplier(scorer);
        }

        private Map<Integer, FixedBitSet> ensureShardScan(LeafReaderContext context, LanceFragmentLeafReader leaf) throws IOException {
            Map<Integer, FixedBitSet> cached = shardMatches.get();
            if (cached != null) {
                return cached;
            }
            // Walk up to the top-level context: LeafReaderContext.leaves()
            // is only valid on the top-level IndexReaderContext. The
            // fragment coordinator's searcher wraps exactly the leaves
            // this per-node executor was assigned, so the sibling set
            // is the fragment subset the request was fanned out with.
            org.apache.lucene.index.IndexReaderContext topCtx = context;
            while (!topCtx.isTopLevel) {
                topCtx = topCtx.parent;
            }
            Map<Integer, FixedBitSet> matchesByFragment = new HashMap<>();
            Map<Integer, LanceFragmentLeafReader> leavesByFragment = new HashMap<>();
            java.util.List<Integer> fragmentIds = new java.util.ArrayList<>();
            for (LeafReaderContext sibling : topCtx.leaves()) {
                LanceFragmentLeafReader sl = LanceFragmentLeafReader.unwrap(sibling.reader());
                if (sl != null) {
                    fragmentIds.add(sl.fragmentId());
                    leavesByFragment.put(sl.fragmentId(), sl);
                    matchesByFragment.put(sl.fragmentId(), new FixedBitSet(sl.maxDoc()));
                }
            }
            if (fragmentIds.isEmpty()) {
                shardMatches.compareAndSet(null, matchesByFragment);
                return shardMatches.get();
            }
            int scanLimit = query().scanLimit();
            ScanOptions.Builder builder = new ScanOptions.Builder().fragmentIds(fragmentIds)
                .filter(query().filterSql())
                .columns(Collections.emptyList())
                .withRowAddress(true);
            if (scanLimit != SCAN_LIMIT_UNBOUNDED) {
                builder = builder.limit(scanLimit);
            }
            try (LanceScanner scanner = leaf.dataset().newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch()) {
                    cancellation.checkCancelled();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long addr = rowAddr.get(i);
                        int fragId = (int) (addr >>> 32);
                        FixedBitSet matches = matchesByFragment.get(fragId);
                        if (matches != null) {
                            // The decoded offset is a physical row; the
                            // bit is set at the row's parent doc id
                            // (identity unless the table has nested
                            // columns).
                            matches.set(leavesByFragment.get(fragId).docOfRow((int) (addr & 0xFFFFFFFFL)));
                        }
                    }
                }
            } catch (IOException | TaskCancelledException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            shardMatches.compareAndSet(null, matchesByFragment);
            return shardMatches.get();
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            // Same rationale as LanceFtsQuery: the scan reads live
            // Lance data whose visibility may advance between
            // searches. Skip Lucene's query cache and let the
            // reader lifecycle handle freshness.
            return false;
        }
    }
}
